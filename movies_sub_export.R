library(tidyverse)
library(stringr)
library(DBI)
library(progress)

conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")

# unzipping subtitles
zipF <- list.files(path = "./subs", pattern = "*.zip", full.names = TRUE)
walk(zipF, ~ unzip(zipfile = .x, exdir = "./unzip"))

# clearing useless files
unzipF <- list.files(path = "./unzip", full.names = TRUE)
subsF <- grep("\\.srt$", unzipF)
files_to_delete <- unzipF[-subsF]
files_to_save <- unzipF[subsF]
walk(files_to_delete, ~ file.remove(.x))

# making pretty movie name from subtitles files names
sub_names <- files_to_save %>% 
  map_chr(~ str_remove_all(.x, "\\.\\/unzip\\/|\\.srt$"))

movie_names <- sub_names %>% 
  map_chr(tolower) %>% 
  map_chr(~ str_remove_all(.x, pattern = "\\[.*?\\]|dvd.*|hd.*|sd.*|eng.*|rus.*")) %>% 
  map_chr(~ str_replace_all(.x, pattern = "\\.", replacement = " ")) %>% 
  map_chr(str_squish)

# writing movie meta data into DB
max_movie_id <- 
  DBI::dbGetQuery(
    conn,
    "SELECT coalesce(max(sid), 0) FROM text_sources"
  ) %>% 
  pull()

tibble(
  name = movie_names,
  file = sub_names
) %>% 
mutate(sid = row_number() + !!max_movie_id, .before = 1L) %>% 
dbAppendTable(conn, "text_sources", .)


# Georgian Unicode-symbols dict 
georgian_unicode <- 
  c(
    0x10D0, 0x10D1, 0x10D2, 0x10D3, 0x10D4, 0x10D5, 0x10D6, 0x10D7, 0x10D8, 0x10D9,
    0x10DA, 0x10DB, 0x10DC, 0x10DD, 0x10DE, 0x10DF, 0x10E0, 0x10E1, 0x10E2, 0x10E3,
    0x10E4, 0x10E5, 0x10E6, 0x10E7, 0x10E8, 0x10E9, 0x10EA, 0x10EB, 0x10EC, 0x10ED,
    0x10EE, 0x10EF, 0x10F0, 0x10F1, 0x10F2, 0x10F3, 0x10F4, 0x10F5, 0x10F6, 0x10F7,
    0x10F8, 0x10F9, 0x10FA, 0x10FB, 0x10FC
  )

georgian_unicode_table <- 
  tibble(unicode = georgian_unicode) %>% 
  mutate(
    string = tolower(sprintf("\\u%04X", unicode + 0x1C90 - 0x10D0)),
    letter = map(unicode, intToUtf8)
  ) %>% 
  unnest(letter)

pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = length(files_to_save)
)


for (movie_id in 1:length(files_to_save)) {

  # parsing subtitles into sentences
  raw_srt_lines <- read_lines(files_to_save[movie_id]) %>% srt::srt_text()
  raw_srt_lines2 <- raw_srt_lines %>% 
    map_chr(str_replace_all, pattern = "<i>|<b>", replacement = "\\(") %>% 
    map_chr(str_replace_all, pattern = "</i>|</b>", replacement = "\\)") %>% 
    map(str_split, pattern = "\n") %>% 
    unlist()
  
  # lines with georgian letters
  ka_txt_vector <- map_lgl(raw_srt_lines2, ~ str_detect(.x, pattern = "[ა-ჰ]"))
  
  
  if (sum(ka_txt_vector) > 0) {
    
    raw_srt_txt <- raw_srt_lines2[ka_txt_vector] %>% 
      reduce(paste0) %>% 
      str_replace_all("(!)", "\\!\\.") %>% 
      str_replace_all("(\\?)", "\\?\\.") %>% 
      str_replace_all("(- )", "\\.- ") %>% 
      str_replace_all("\\.\\.\\.", "\u2026") %>% 
      str_remove_all("\\[.*?\\]")
    
    # replacing symbols
    for (i in 1:nrow(georgian_unicode_table)) {
      raw_srt_txt <- str_replace_all(raw_srt_txt, georgian_unicode_table$string[i], georgian_unicode_table$letter[i])
    }
    
    # all sentences from subtitles
    movies_sentences <- raw_srt_txt %>% 
      str_split("\\.") %>% 
      unlist() %>% 
      map_chr(str_squish) %>% 
      .[map_int(., str_length) > 0] %>% 
      .[!map_lgl(., str_detect, pattern = "[^ა-ჰ[:space:][:punct:][:digit:]]+")] 
      
    movie_intro <- 1:if_else(length(movies_sentences) > 25, 25, 1)
    
    # all Georgian words from subtitles
    movies_words <- movies_sentences[-movie_intro] %>% 
      map(~ str_split(.x, "\\s+")) %>% 
      unlist() %>% 
      str_replace_all("[[:punct:]]", "") %>% 
      as_tibble() %>% 
      filter(str_detect(value, pattern = "[ა-ჰ]")) %>%
      count(value) %>% 
      arrange(desc(n))
    
    tibble(
      stype = "film",
      sid = !!movie_id + !!max_movie_id,
      txt = movies_sentences[-movie_intro]
    ) %>% 
    distinct() %>% 
    dbAppendTable(conn, "ka_sentences", .)
    
    tibble(
      stype = "film",
      sid = !!movie_id + !!max_movie_id,
      movies_words
    ) %>% 
    rename(wrd = value, frq = n) %>% 
    dbAppendTable(conn, "ka_words", .)
  
  }
  
  pb$tick()
}
