library(tidyverse)
library(rvest)
library(stringr)
library(DBI)
library(progress)

# reading html ignoring errors
read_html_iter <- function(web_link, max_attempt = 10) {
  
  for (i in 1:max_attempt) {
    export_html <- try(xml2::read_html(web_link), silent = TRUE)
    if (class(export_html)[1] != "try-error") break
  }
  
  export_html
}

# reading text from random wiki-page
random_wiki_page_url <- "https://ka.wikipedia.org/wiki/Special:Random"
load_text_from_wki <- function(link = random_wiki_page_url) {
  page <- read_html_iter(random_wiki_page_url)
  ourl <- html_attr(html_nodes(page, xpath = '//link[@rel="canonical"]'), "href")
  durl <- URLdecode(ourl) %>% iconv("UTF-8", "UTF-8") 
  text <- html_text(html_nodes(page, "#mw-content-text p"))
  head <- html_text(html_nodes(page, "h1#firstHeading"))
  list(head = head, text = text, link = durl)
}

# cleaning text from wiki
text_to_sentences <- function(text) {
  text %>% 
    reduce(paste0) %>%
    str_remove_all("\\((.*?)\\)") %>% 
    str_remove_all("\\[(.*?)\\]") %>% 
    str_remove_all("\n") %>% 
    str_replace_all(pattern = '([[:space:]])([[:space:]])', replacement = "\\1") %>%
    str_replace_all("([[:space:]])(\\,)", "\\2") %>% 
    str_replace_all("( )([ა-ჰa-zA-Z])\\.", "\\1\\2") %>% 
    str_replace_all("(!)", "\\!\\.") %>% 
    str_replace_all("(\\?)", "\\?\\.") %>% 
    str_replace_all("\\.\\.\\.", "\u2026") %>%
    str_split("\\.") %>%
    unlist() %>% 
    str_squish() %>% 
    str_replace_all("( )([ა-ჰa-zA-Z])( )", "\\1\\2\\.\\3") %>% 
    .[map_int(., str_length) > 0] %>% 
    .[!map_lgl(., str_detect, pattern = "[^ა-ჰ[:space:][:punct:][:digit:]]+")] %>% 
    .[!map_lgl(., str_detect, pattern = "\u00B7")]
}


# parcing data into DB
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
wiki_iter_cnt <- 5000L
pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = wiki_iter_cnt
)


for (i in 1:wiki_iter_cnt) {
  
  wiki <- load_text_from_wki()
  existing_sources <- dbGetQuery(conn, "SELECT name FROM text_sources WHERE stype = 'wiki'") %>% pull()
  max_source_id <- dbGetQuery(conn, "SELECT coalesce(max(sid), 0) FROM text_sources") %>% pull()
  
  if (!wiki$head %in% existing_sources) {
  
    wiki_head <- wiki$head
    wiki_link <- wiki$link
    
    if (length(wiki$text) != 0) {
      
      sentences <- wiki$text %>% text_to_sentences()
      words <- sentences %>% 
        str_split("\\s+") %>% 
        unlist() %>% 
        str_replace_all("[[:punct:]]", "") %>% 
        as_tibble() %>% 
        filter(str_detect(value, pattern = "[ა-ჰ]")) %>%
        count(value) %>% 
        arrange(desc(n))
      
      # Info about source
      dbAppendTable(
        conn, 
        "text_sources", 
        tibble(
          sid = !!max_source_id + 1L,
          name = !!wiki_head,
          file = !!wiki_link,
          stype = "wiki"
        )
      )
      
      # Cleared sentences
      dbAppendTable(
        conn, 
        "ka_sentences", 
        tibble(
          sid = !!max_source_id + 1L,
          txt = sentences
        ) %>% distinct()
      )
      
      # Cleared words
      dbAppendTable(
        conn, 
        "ka_words", 
        tibble(
          sid = !!max_source_id + 1L,
          words
        ) %>% rename(wrd = value, frq = n) 
      )
    }
  }
  
  pb$tick()
}

dbDisconnect(conn)
