library(tidyverse)
library(stringr)
library(DBI)
library(rvest)
library(progress)


conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_raw_word_dict")

# read html ignoring errors
read_html_iter <- function(web_link, max_attempt = 10) {
  for (i in 1:max_attempt) {
    export_html <- try(xml2::read_html(web_link), silent = TRUE)
    if (class(export_html)[1] != "try-error") break
  }

  export_html
}


# the most popular words
top_words <- raw_ka_words %>%
  arrange(id) %>%
  head(1000L)


# exporting data from ganmarteba dictionary
ganmarteba_quick <- function(wrd, web_link) {
  
  ganmarteba_html <- read_html_iter(web_link)
  next_page_links <- ganmarteba_html %>%
    html_nodes("a") %>%
    html_attr("href") %>% 
    as_tibble() %>% 
    filter(str_detect(value, "word") & !str_detect(value, "facebook")) %>% 
    pull() %>% 
    map_chr(~ iconv(URLdecode(.x), "UTF-8", "UTF-8")) %>% 
    map_chr(~ paste0("https://www.ganmarteba.ge", .x))
  
  next_pages <- as.character()
  word_from_site <- html_text(html_nodes(ganmarteba_html, "h1"))[1]
  jump_link <- html_text(html_nodes(ganmarteba_html, ".firstpolp"))[1] %>% 
    str_detect("^იხ.")
  
  if (is.na(word_from_site)) {
    next_pages <- next_page_links[str_detect(next_page_links, paste0("/", wrd, "/[[:digit:]]$"))]
  }
  
  if (jump_link & !is.na(jump_link)) {
    next_pages <- next_page_links[1]
  }

  list(link = next_pages, html = ganmarteba_html)
}


pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = nrow(top_words)
)

ganmarteba_words_list <- list()
ganmarteba_extra_list <- list()
k <- 1L

for (i in top_words$id) {

  wrd <- filter(top_words, id == !!i) %>% pull(wrd)
  web_link <- paste0("https://www.ganmarteba.ge/word/", wrd)
  ganmarteba <- ganmarteba_quick(wrd, web_link)
  
  list1 <- list()
  list2 <- list()
  
  if (length(ganmarteba$link) > 0) {
    
    for (j in 1:length(ganmarteba$link)) {
      list1[[j]] <- ganmarteba$link[j]
      list2[[j]] <- ganmarteba_quick(wrd, ganmarteba$link[j])$html
    }
    
  } else {
    list1[[1]] <- web_link
    list2[[1]] <- ganmarteba$html
  }
  
  for (j in 1:NROW(list1)) {
    
    ganmarteba_html <- list2[[j]]
    word_from_site <- html_text(html_nodes(ganmarteba_html, "h1"))[1]
    p_html_text <- html_text(html_nodes(ganmarteba_html, "p"))
    part_of_speach <- p_html_text[1]
    verbs <- p_html_text[str_detect(p_html_text, "^ზმნები:")][1] %>% 
      str_remove("^ზმნები:") %>% 
      str_squish()
    relative <- p_html_text[str_detect(p_html_text, "^ნათესაობითი ბრუნვა:")][1] %>% 
      str_remove("^ნათესაობითი ბრუნვა:") %>% 
      str_squish()  
    
    if (!is.na(word_from_site)) {
      ganmarteba_words_list[[k]] <- 
        tibble(
          id  = i,
          gid = k,
          word = word_from_site,
          pos0 = part_of_speach,
          relative = relative,
          verbs = verbs,
          site = list1[[j]]
        )
      
      wrd_meaning <- html_text(html_nodes(ganmarteba_html, ".definition"))
      wrd_example <- html_text(html_nodes(ganmarteba_html, ".illustracion"))
      wrd_data_length <- min(length(wrd_meaning), length(wrd_example))
      ganmarteba_extra_list[[k]] <-
        tibble(
          gid = k,
          meaning = wrd_meaning[1:wrd_data_length],
          examples = wrd_example[1:wrd_data_length]
        ) %>% 
        mutate(mid = row_number(), .after = 1L)
      k <- k + 1L
    } 
  }
  pb$tick()
}


# words from dictionary
ganmarteba_words <- 
  ganmarteba_words_list %>% 
  reduce(add_row) %>% 
  mutate(word = str_remove(word, "[[:digit:]]$"))

# details about part of speech
part_of_speach_dict <-
  tribble(
    ~eng,    ~eng2,		       ~geo,
    "noun",	 "Noun",		     "არსებითი სახელი",
    "adj",   "Adjective",    "ზედსართავი სახელი",
    "adv",   "Adverb",		   "ზმნიზედა", # наречие
    "pron",  "Pronoun",		   "ნაცვალსახელი", # местоимение
    "verb",	 "Verb",			   "ზმნა",
    "conj",  "Conjunction",	 "კავშირი", # союзы
    "num",   "Number",		   "რიცხვითი სახელი",
    "prep",  "Preposition",  "თანდებული", # послеслоги
    "part",	 "Participle",	 "ნაწილაკი", # частицы
    "excl",  "Exclamation",  "შორისდებული" # восклицания
  )
pos_index <- map_int(ganmarteba_words$pos0, ~ which(str_detect(.x, part_of_speach_dict$geo))[1])
ganmarteba_words_ext <- ganmarteba_words %>% 
  mutate(pos1 = !!part_of_speach_dict[pos_index,]$eng, .before = "pos0") %>% 
  mutate_at(vars(pos0, pos1), ~if_else(is.na(pos1), as.character(NA), .x))

# meaning and examples
ganmarteba_extra <- 
  ganmarteba_extra_list %>% 
  reduce(add_row) %>% 
  mutate(meaning = str_replace_all(meaning, "\\([[:digit:]]\\)", " "))

