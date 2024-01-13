library(tidyverse)
library(stringr)
library(DBI)
library(rvest)
library(progress)

conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
dbExecute(conn, {
    "
    CREATE TABLE ka_raw_word_dict AS
    SELECT row_number() over (order by frq desc) as id
    , t.*
    FROM 
    (
      SELECT coalesce(p.wrd, t2.wrd) as wrd
      , sum(t2.frq) as frq
      , count(distinct t0.sid) as srcs
      , sum(case when t0.stype = 'book' then t2.frq end) as frq_book
      , sum(case when t0.stype = 'film' then t2.frq end) as frq_film
      , sum(case when t0.stype = 'wiki' then t2.frq end) as frq_wiki
      , count(distinct case when t0.stype = 'book' then t0.sid end) as book_srcs
      , count(distinct case when t0.stype = 'film' then t0.sid end) as film_srcs
      , count(distinct case when t0.stype = 'wiki' then t0.sid end) as wiki_srcs
      FROM ka_words t2 
      JOIN text_sources t0 ON t0.sid = t2.sid
      LEFT JOIN ka_pseudo_words p on t2.wrd = p.wrd2
      GROUP BY coalesce(p.wrd, t2.wrd)
    ) t
    WHERE book_srcs + film_srcs > 5 or wiki_srcs > 100
    ORDER BY frq DESC
    "})
dbExecute(conn, {
  "
    CREATE TABLE ka_raw_word_meta AS
    SELECT 
      case when book_srcs + film_srcs > 5 or wiki_srcs > 100 
        then 'normal' else 'exotic' 
      end as wtype
    , sum(frq) as frq
    , sum(frq_book) as frq_book
    , sum(frq_film) as frq_film
    , sum(frq_wiki) as frq_wiki
    FROM 
    (
      SELECT coalesce(p.wrd, t2.wrd) as wrd
      , sum(t2.frq) as frq
      , count(distinct t0.sid) as srcs
      , sum(case when t0.stype = 'book' then t2.frq end) as frq_book
      , sum(case when t0.stype = 'film' then t2.frq end) as frq_film
      , sum(case when t0.stype = 'wiki' then t2.frq end) as frq_wiki
      , count(distinct case when t0.stype = 'book' then t0.sid end) as book_srcs
      , count(distinct case when t0.stype = 'film' then t0.sid end) as film_srcs
      , count(distinct case when t0.stype = 'wiki' then t0.sid end) as wiki_srcs
      FROM ka_words t2 
      JOIN text_sources t0 ON t0.sid = t2.sid
      LEFT JOIN ka_pseudo_words p on t2.wrd = p.wrd2
      GROUP BY coalesce(p.wrd, t2.wrd)
    ) t
    GROUP BY
      case when book_srcs + film_srcs > 5 or wiki_srcs > 100 
        then 'normal' else 'exotic' 
      end
    "})
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_raw_word_dict")
raw_ka_txt <- dbGetQuery(conn,
            "
select t1.*, t0.name
from ka_sentences t1
JOIN text_sources t0 ON t0.sid = t1.sid
WHERE t0.stype = 'film'
            "
            )


raw_ka_txt %>% 
  filter(str_detect(txt, "არსებობს")) %>% 
  view(title = "txt") 

raw_ka_words %>% 
  arrange(desc(frq_film)) %>% 
  mutate(rn = row_number(), .before = 1L) %>% 
  slice(1:500) %>% 
  view()

# read html ignoring errors
read_html_iter <- function(web_link, max_attempt = 10) {
  
  for (i in 1:max_attempt) {
    export_html <- try(xml2::read_html(web_link), silent = TRUE)
    if (class(export_html)[1] != "try-error") break
  }
  
  export_html
}

top_words <- raw_ka_words %>% 
  arrange(desc(frq)) %>% 
  head(100L)

k <- 1L
i <- 35L
wrd <- top_words[i,]$wrd
web_link <- paste0("https://www.ganmarteba.ge/word/", wrd)
ganmarteba_html <- read_html_iter(web_link)
word_from_site <- html_text(html_nodes(ganmarteba_html, "h1"))[1]
part_of_speach <- html_text(html_nodes(ganmarteba_html, "p"))[1]
wrd_meaning <- html_text(html_nodes(ganmarteba_html, ".definition"))
wrd_example <- html_text(html_nodes(ganmarteba_html, ".illustracion"))
wrd_data_length <- min(length(wrd_meaning), length(wrd_example))
wrd_details <- 
  tibble(
    meaning = wrd_meaning[1:wrd_data_length], 
    examples = wrd_example[1:wrd_data_length]
  )