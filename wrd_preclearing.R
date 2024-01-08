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

# manual clearing
dbExecute(conn, "DELETE FROM ka_pseudo_words WHERE LENGTH(wrd) = 1")
dbExecute(
  conn, 
  "
  DELETE FROM ka_pseudo_words 
  WHERE id in 
  (
    with all_ka_words as
    (
      SELECT wrd
      , sum(frq) as frq
      FROM ka_words t2 
      JOIN text_sources t0 ON t0.sid = t2.sid
      GROUP BY wrd
    )
    select a.id
    from ka_pseudo_words a
    join all_ka_words b1 on a.wrd  = b1.wrd
    join all_ka_words b2 on a.wrd2 = b2.wrd
    where 1=1 
      and b1.frq < b2.frq 
      and (b2.frq / b1.frq > 2 and a.type = 'aris' or a.type in ('echo', 'also'))
  )"
)

dbExecute(
  conn, 
  "
  DELETE FROM ka_pseudo_words 
  WHERE id in 
  (
    with all_ka_words as
    (
      SELECT wrd
      , sum(frq) as frq
      FROM ka_words t2 
      JOIN text_sources t0 ON t0.sid = t2.sid
      GROUP BY wrd
    )
    select id
    from 
    (
      select a.*
        , row_number() over (partition by a.wrd2 order by b1.frq desc) as rn
      from ka_pseudo_words a
      join all_ka_words b1 on a.wrd  = b1.wrd
    )
    where rn > 1
  )"
)

dbDisconnect(conn)
