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

top_words <- raw_ka_words %>%
  arrange(id) %>%
  head(100L)


part_of_speach_dict <-
  tribble(
    ~eng,    ~eng2,		       ~geo,
    "n",	   "Noun",		     "არსებითი სახელი",
    "adj",   "Adjective",    "ზედსართავი სახელი",
    "adv",   "Adverb",		   "ზმნიზედა",
    "pron",  "Pronoun",		   "ნაცვალსახელი",
    "v",	   "Verb",			   "ზმნა",
    "conj",  "Conjunction",	 "კავშირი",
    "num",   "Number",		   "რიცხვითი სახელი",
    "postp", "Postposition", "თანდებული",
    "part",	 "Participle",	 "ნაწილაკი"
  )


ganmarteba_words_list <- list()
ganmarteba_extra_list <- list()
k <- 1L

pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = nrow(top_words)
)

ganmarteba_quick <- function(web_link) {
  
  ganmarteba_html <- read_html_iter(web_link)
  next_page_links <- ganmarteba_html %>%
    html_nodes("a") %>%
    html_attr("href") %>% 
    as_tibble() %>% 
    filter(str_detect(value, "word") & !str_detect(value, "facebook")) %>% 
    pull() %>% 
    map_chr(~ iconv(URLdecode(.x), "UTF-8", "UTF-8")) %>% 
    map_chr(~ paste0("https://www.ganmarteba.ge", .x))
  
  word_from_site <- html_text(html_nodes(ganmarteba_html, "h1"))[1]
  ih <- html_text(html_nodes(ganmarteba_html, ".firstpolp"))[1] %>% 
    str_detect("^იხ.")
  
  if (is.na(word_from_site)) {
    next_pages <- next_page_links[str_detect(next_page_links, '[[:digit:]]$')]
  }
  
  if (ih) {
    next_pages <- next_page_links[1]
  }

  list(link = next_pages, html = ganmarteba_html)
}

for (i in top_words$id) {

  wrd <- filter(top_words, id == !!i) %>% pull(wrd)
  
  web_link <- paste0("https://www.ganmarteba.ge/word/", wrd)
  ganmarteba <- ganmarteba_quick(web_link)
  
  list1 <- list()
  list2 <- list()
  
  if (length(ganmarteba$link) > 0) {
    
    for (j in 1:length(ganmarteba$link)) {
      list1[[j]] <- ganmarteba$link[j]
      list2[[j]] <- ganmarteba_quick(ganmarteba$link[j])$html
    }
    
  } else {
    list1[[1]] <- web_link
    list1[[2]] <- ganmarteba$html
  }
  
  
  ganmarteba_html <- ganmarteba$html
  word_from_site <- html_text(html_nodes(ganmarteba_html, "h1"))[1]
  part_of_speach <- html_text(html_nodes(ganmarteba_html, "p"))[1]
  
  if (!is.na(word_from_site)) {
    ganmarteba_words_list[[k]] <- 
      tibble(
        id = i,
        word = word_from_site,
        site = web_link,
        pos0 = part_of_speach
      )
    
    wrd_meaning <- html_text(html_nodes(ganmarteba_html, ".definition"))
    wrd_example <- html_text(html_nodes(ganmarteba_html, ".illustracion"))
    wrd_data_length <- min(length(wrd_meaning), length(wrd_example))
    ganmarteba_extra_list[[k]] <-
      tibble(
        id = i,
        meaning = wrd_meaning[1:wrd_data_length],
        examples = wrd_example[1:wrd_data_length]
      )
    k <- k + 1L
  }
  
  pb$tick()
}

ganmarteba_words <- 
  ganmarteba_words_list %>% 
  reduce(add_row)

pos_index <- map_int(ganmarteba_words$pos0, ~ which(str_detect(.x, part_of_speach_dict$geo))[1])
ganmarteba_words_ext <- ganmarteba_words %>% 
  add_column(pos1 = part_of_speach_dict[pos_index,]$eng2)

ganmarteba_extra <- 
  ganmarteba_extra_list %>% 
  reduce(add_row)

top_words %>% 
  select(id, wrd) %>% 
  inner_join(ganmarteba_words, by = "id") %>% 
  left_join(ganmarteba_extra, by = "id") %>% 
  view()
