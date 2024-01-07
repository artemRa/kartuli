library(tidyverse)
library(stringr)
library(DBI)


conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
all_ka_words <- dbGetQuery(conn, "SELECT DISTINCT wrd FROM ka_words")

pseudo_word_candidates <- all_ka_words %>% 
  as_tibble() %>%
  filter(str_detect(wrd, pattern = "(ო|ც|ა)$"))


aend_words <- all_ka_words %>%
  mutate(
    wrd2 = case_when(
      str_detect(wrd, "[იოუეა]$") ~ paste0(wrd, "ა"),
      T ~ paste0(wrd, "აა")
    )
  ) %>% 
  inner_join(pseudo_word_candidates, by = c("wrd2" = "wrd"))


oend_words <- all_ka_words %>% 
  mutate(wrd2 = paste0(wrd, "ო")) %>% 
  inner_join(pseudo_word_candidates, by = c("wrd2" = "wrd"))
  
tend_words <- all_ka_words %>% 
  mutate(wrd2 = paste0(wrd, "ც")) %>% 
  inner_join(pseudo_word_candidates, by = c("wrd2" = "wrd"))

pseudo_words <- 
  rbind(
    add_column(aend_words, type = "aris"),
    add_column(oend_words, type = "echo"),
    add_column(tend_words, type = "also")
  )
  
dbAppendTable(
  conn, 
  "ka_pseudo_words", 
  pseudo_words
)

dbDisconnect(conn)
