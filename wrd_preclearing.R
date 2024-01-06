library(tidyverse)
library(stringr)
library(DBI)
library(progress)


conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
all_ka_words <- dbGetQuery(conn, "SELECT DISTINCT wrd FROM ka_words")

aend_words <- all_ka_words %>% 
  as_tibble() %>%
  filter(
    str_detect(wrd, "[იოუეა]ა$") |
    str_detect(wrd, "[^იოუეა]აა$")
  )

oend_words <- all_ka_words %>% 
  as_tibble() %>%
  filter(str_detect(wrd, "ო$"))

tsend_words <- all_ka_words %>% 
  as_tibble() %>%
  filter(str_detect(wrd, "ც$"))


verified_ka_words <- 
  dbGetQuery(
    conn, 
    "
    SELECT wrd
    FROM 
    (
      SELECT wrd
      , sum(frq) as frq
      , count(distinct case when t0.stype in ('book', 'film') then t0.sid end) as media
      , count(distinct case when t0.stype = 'wiki' then t0.sid end) as wiki
      FROM ka_words t2 
      JOIN text_sources t0 ON t0.sid = t2.sid
      GROUP BY wrd
    ) t
    WHERE (media > 5 or wiki > 100) 
    "
  )

aend_verified_words <- verified_ka_words %>%
  mutate(
    wrd2 = case_when(
      str_detect(wrd, "[იოუეა]$") ~ paste0(wrd, "ა"),
      T ~ paste0(wrd, "აა")
    )
  ) %>% 
  inner_join(aend_words, by = c("wrd2" = "wrd"))

oend_verified_words <- verified_ka_words %>% 
  mutate(wrd2 = paste0(wrd, "ო")) %>% 
  inner_join(oend_words, by = c("wrd2" = "wrd"))
  
tsend_verified_words <- verified_ka_words %>% 
  mutate(wrd2 = paste0(wrd, "ც")) %>% 
  inner_join(tsend_words, by = c("wrd2" = "wrd"))

view(tsend_verified_words)  
