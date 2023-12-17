library(tidyverse)
library(stringr)

zipF <- list.files(path = "./subs", pattern = "*.zip", full.names = TRUE)
walk(zipF, ~ unzip(zipfile = .x, exdir = "./unzip"))

unzipF <- list.files(path = "./unzip", full.names = TRUE)
subsF <- grep("\\.srt$", unzipF)
files_to_delete <- unzipF[-subsF]
files_to_save <- unzipF[subsF]
walk(files_to_delete, ~ file.remove(.x))

movie_names <- files_to_save %>% 
  map_chr(tolower) %>% 
  map_chr(~ str_remove_all(.x, pattern = ".*/|\\.srt$|\\[.*?\\]|dvd.*|hd.*|sd.*|eng.*|rus.*")) %>% 
  map_chr(~ str_replace_all(.x, pattern = "\\.", replacement = " ")) %>% 
  map_chr(str_squish)

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


raw_srt_lines <- read_lines(files_to_save[3]) %>% srt::srt_text()
raw_srt_lines2 <- raw_srt_lines %>% 
  map_chr(str_replace_all, pattern = "<i>|<b>", replacement = "\\(") %>% 
  map_chr(str_replace_all, pattern = "</i>|</b>", replacement = "\\)") %>% 
  map(str_split, pattern = "\n") %>% 
  unlist()

ka_txt_vector <- map_lgl(raw_srt_lines2, ~ str_detect(.x, pattern = "[ა-ჰ]"))
sum(ka_txt_vector)

raw_srt_txt <- raw_srt_lines2[ka_txt_vector] %>% 
  reduce(paste0) %>% 
  str_replace_all("(!)", "\\!\\.") %>% 
  str_replace_all("(\\?)", "\\?\\.") %>% 
  str_replace_all("(- )", "\\.- ") %>% 
  str_replace_all("\\.\\.\\.", "\u2026") %>% 
  str_remove_all("\\[.*?\\]")

for (i in 1:nrow(georgian_unicode_table)) {
  raw_srt_txt <- str_replace_all(raw_srt_txt, georgian_unicode_table$string[i], georgian_unicode_table$letter[i])
}

movies_sentences <- raw_srt_txt %>% 
  str_split("\\.") %>% 
  unlist() %>% 
  map_chr(str_squish) %>% 
  .[map_int(., str_length) > 0] %>% 
  .[!map_lgl(., str_detect, pattern = "[^ა-ჰ[:space:][:punct:][:digit:]]+")] 
  

movies_sentences %>% 
  map(~ str_split(.x, "\\s+")) %>% 
  unlist() %>% 
  str_replace_all("[[:punct:]]", "") %>% 
  as_tibble() %>% 
  filter(str_detect(value, pattern = "[ა-ჰ]")) %>%
  count(value) %>% 
  arrange(desc(n)) %>% 
  view()