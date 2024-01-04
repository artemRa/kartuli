library(pdftools)
library(tidyverse)
library(stringr)
library(DBI)
library(progress)


conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
books_dir <- list.files(path = "./books", full.names = TRUE)
pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = length(books_dir)
)

for (j in 1:length(books_dir)) {
  
  dir <- books_dir[j]
  raw_pdf_txt <- pdftools::pdf_text(dir)
  
  book_file <- str_remove(dir, "^\\./books/")
  book_name <- str_remove(book_file, "\\.pdf$") %>% 
    str_replace_all("-|_", " ")
  
  existing_sources <- dbGetQuery(conn, "SELECT file FROM text_sources WHERE stype = 'book'") %>% pull()
  
  if (!book_file %in% existing_sources) {
    
    txt_length <- raw_pdf_txt %>% map_int(str_length)
    first_txt_page <- which(txt_length > median(txt_length) / 10)[1]
    
    max_source_id <- dbGetQuery(conn, "SELECT coalesce(max(sid), 0) FROM text_sources") %>% pull()
    
    sentences <- list()
    words <- list()
    i <- 1L
    
    for (page in first_txt_page:length(txt_length)) {
    
      raw_one_page <- raw_pdf_txt[page] %>% 
        reduce(paste0) %>% 
        str_split("\n") %>% 
        unlist() %>% 
        str_squish()
      
      page_txt_length <- str_length(raw_one_page)
      
      if (sum(page_txt_length) > 0) {
        trust_interval <- which(page_txt_length > 0.8 * median(page_txt_length))
        raw_one_page_trust <- raw_one_page[min(trust_interval):max(trust_interval)]
        
        one_page_sentences <- raw_one_page_trust %>%
          reduce(paste) %>% 
          str_replace_all("([ა-ჰ])(-)( )([ა-ჰ])", "\\1\\3\\4") %>% 
          str_replace_all("( )([ა-ჰa-zA-Z])\\.", "\\1\\2") %>% 
          str_remove_all("\\((.*?)\\)") %>% 
          str_remove_all("\\[(.*?)\\]") %>% 
          str_replace_all("\\.\\.\\.", "\u2026") %>% 
          str_replace_all("([ა-ჰa-zA-Z])([:digit:])", "\\1") %>%
          str_replace_all("([[:space:]])([[:space:]])", "\\1") %>%
          str_replace_all("(\\?!)", "\\?") %>% 
          str_replace_all("(!)", "\\!\\.") %>% 
          str_replace_all("(\\?)", "\\?\\.") %>% 
          str_remove("[:digit:][ა-ჰa-zA-Z](.*?)$") %>% 
          str_split("\\.") %>%
          unlist() %>% 
          str_squish() %>% 
          str_replace_all("( )([ა-ჰa-zA-Z])( )", "\\1\\2\\.\\3") %>% 
          .[map_int(., str_length) > 1] %>% 
          .[-c(1L, length(.))] %>% 
          .[!map_lgl(., str_detect, pattern = "[^ა-ჰ[:space:][:punct:][:digit:]]+")]
        
        one_page_words <- one_page_sentences %>% 
          str_split("\\s+") %>% 
          unlist() %>% 
          str_replace_all("[[:punct:]]", "") %>% 
          as_tibble() %>% 
          filter(str_detect(value, pattern = "[ა-ჰ]")) %>%
          count(value)
        
        if (length(one_page_sentences) > 0) {
          
          sentences[[i]] <- one_page_sentences
          words[[i]] <- one_page_words
          i <- i + 1L 
        }
      }
    }
    
    if (!is_empty(words)) {
      
      # info about source
      dbAppendTable(
        conn, 
        "text_sources", 
        tibble(
          sid = !!max_source_id + 1L,
          name = !!book_name,
          file = !!book_file,
          stype = "book"
        )
      )
      
      # cleared sentences
      dbAppendTable(
        conn, 
        "ka_sentences", 
        tibble(
          sid = !!max_source_id + 1L,
          txt = unlist(sentences)
        ) %>% distinct()
      )
      
      export_words <- words %>% 
        reduce(add_row) %>% 
        group_by(value) %>% 
        summarise(n = sum(n)) %>% 
        arrange(desc(n)) %>% 
        rename(wrd = value, frq = n)
      
      # cleared words
      dbAppendTable(
        conn, 
        "ka_words", 
        tibble(
          sid = !!max_source_id + 1L,
          export_words
        )
      ) 
    }
  }
  
  pb$tick()
}

dbDisconnect()
