# Settings ----
library(tidyverse)
library(stringr)
library(DBI)
library(rvest)
library(progress)
library(gsheet)

# yaml secrets
config <- yaml::read_yaml("secret.yaml")

# dictionaries from Google table
verb_tense_data  <- gsheet::gsheet2tbl(config$glink1)
verb_forms_data  <- gsheet::gsheet2tbl(config$glink3)
extra_word_forms <- gsheet::gsheet2tbl(config$glink2)
verified_ending  <- gsheet::gsheet2tbl(config$glink4)

# export pre-clearing Georgian words 
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")

# read html ignoring errors
read_html_iter <- function(web_link, max_attempt = 10, ...) {
  for (i in 1:max_attempt) {
    export_html <- try(xml2::read_html(web_link, ...), silent = TRUE)
    if (class(export_html)[1] != "try-error") break
  }
  
  export_html
}


# Old lingua.ge links ----
# verb_query <- "https://lingua.ge/verbs/"
# manual_verb_list <- c("თქმა-2", "ყოფნა", "ჯდომა-სხდომავზივარ", "ფიქრი" )
# extra_links <- paste0(verb_query, manual_verb_list, '/')
# conjugate_meta <- readRDS("conjugate_meta.RData")
# conjugate_meta %>%
#   reduce(add_row) %>% 
#   select(link) %>% 
#   add_row(link = extra_links) %>% 
#   dbAppendTable(conn, "ka_lingua_link", .)


# Parsing verb from lingua.ge ----
# collecting links for verbs
verb_links_collector <- function(start_words) {
  
  lingua_links <- list()
  pb <- progress_bar$new(
    format = "[:bar] :percent ETA: :eta",
    total = length(start_words)
  )
  for (i in 1:length(start_words)) {
    
    wrd <- start_words[i]
    web_link_search <- paste0("https://lingua.ge/?s=", wrd)
    lingua_html_search <- read_html_iter(web_link_search)
    lingua_links[[i]] <- as.character(lingua_html_search) %>% 
      str_extract_all('https://lingua.ge/verbs/(.*)/') %>% 
      unlist() %>%
      map_chr(~ iconv(URLdecode(.x), "UTF-8", "UTF-8")) %>% 
      .[str_length(.) < 100L] %>% 
      .[!str_detect(., "feed")] %>% 
      unique()
    
    pb$tick()
  }
  
  # final banch of links
  lingua_links %>% 
    unlist() %>% 
    unique()
}

already_found_links <- dbGetQuery(conn, "SELECT * FROM ka_lingua_link") %>% pull()

already_found_verbs <- ka_word_tidy_dict %>% 
  filter(pos == "verb", tid == "X000") %>% 
  pull(word)

losty_words <- raw_ka_words %>% 
  arrange(desc(frq)) %>% 
  slice(1:1000) %>% 
  anti_join(ka_word_tidy_dict, by = "wid") %>% 
  pull(wrd)

lingua_links_v1 <- verb_links_collector(already_found_verbs)
lingua_links_v2 <- verb_links_collector(losty_words)
new_lingua_links <- setdiff(unique(lingua_links_v1, lingua_links_v2), already_found_links)


# collecting verb data from lingua.ge
verb_form_collector <- function(banch_of_verb_link) {
  
  pb <- progress_bar$new(
    format = "[:bar] :percent ETA: :eta",
    total = length(banch_of_verb_link)
  )
  
  conjugate_meta <- list()
  conjugate_verbs <- list()
  k <- 1L
  
  for (i in 1:length(banch_of_verb_link)) {
    
    lingua_html <- read_html_iter(banch_of_verb_link[i])
    if (is.list(lingua_html)) {
      
      html_div_block <- html_elements(lingua_html, "div")
      html_div_class <- html_div_block %>% html_attr("class")
      verbs_position <- which(html_div_class == "elementor-widget-container")
      raw_div_vector <- html_div_block[verbs_position] %>% html_text2()
      
      div_start_line <- which(raw_div_vector == "Home » Conjugation")[1]
      clear_div_vector <- raw_div_vector[(div_start_line+1):length(raw_div_vector)]
      
      eng_translation <- str_remove(clear_div_vector[1], "(v.t.i.)|(v.i.)|(v.t.)") %>% str_squish()
      conjugate_meta[[k]] <- 
        tibble(
          meta = clear_div_vector[2*(1:8)+1],
          info = clear_div_vector[2*(1:8)+2]
        ) %>% 
        rbind(c("English", eng_translation), .) %>% 
        filter(meta %in% c("English", "Infinitive", "Preverb", "Participle")) %>% 
        pivot_wider(names_from = meta, values_from = info) %>% 
        rename_all(tolower) %>% 
        add_column(lid = !!k, .before = 1) %>% 
        mutate(link = !!banch_of_verb_link[i])
      
      num_form_vector1 <- c(2:4, 6:8)
      num_form_vector2 <- c(22, 31, 40, 51, 60, 69, 80, 89, 100, 109, 118)
      
      conjugate_tenses <- 
        map(num_form_vector2, 
            ~ tibble(
              word  = clear_div_vector[num_form_vector1 + .x],
              numb = c(rep(1, 3), rep(2, 3)),
              prsn = rep(1:3, 2),
              tense = str_remove_all(clear_div_vector[.x], "[ა-ჰ]") %>% str_squish(),
            )
        ) %>% reduce(add_row)
      
      num_form_vector1 <- c(2:3, 5:7)
      num_form_vector2 <- c(129)
      
      conjugate_imperative <- 
        map(num_form_vector2, 
            ~ tibble(
              word  = clear_div_vector[num_form_vector1 + .x],
              numb  = c(rep(1, 2), rep(2, 3)),
              prsn  = c(2:3, 1:3),
              tense = str_remove_all(clear_div_vector[.x], "[ა-ჰ]") %>% str_squish()
            )
        ) %>% reduce(add_row)
      
      conjugate_verbs[[k]] <- add_row(conjugate_tenses, conjugate_imperative) %>% 
        add_column(lid = !!k, .before = 1) %>% 
        filter(word != "")
      
      k <- k + 1 
    }
    pb$tick()
  }
  
  # verb and meta data output
  list(
    verbs = reduce(conjugate_verbs, add_row),
    meta = reduce(conjugate_meta, add_row)
  )
  
}

conjugation <- new_lingua_links %>% 
  verb_form_collector()

saveRDS(conjugation, "conjugation.RData")

