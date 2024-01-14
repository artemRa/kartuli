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
    ~geo, 				~eng,		~eng2
    "არსებითი სახელი", 	"n", 		"Noun",
    "ზედსართავი სახელი", 	"adj", 		"Adjective",
    "ზმნიზედა", 			"adv", 		"Adverb",
    "ნაცვალსახელი",		"pron", 	"Pronoun",
    "ზმნა",				"v",		"Verb",
    "კავშირი",			"conj", 	"Conjunction",
    "რიცხვითი სახელი",	"num", 		"Number",
    "თანდებული",			"postp",	"Postposition",
    "ნაწილაკი",			"part",		"Participle"
  )



k <- 1L
i <- 35L
wrd <- filter(top_words, id == !!i) %>% pull(wrd)
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

wrd_details %>% view()

tribble(
  ~geo, ~eng,
  "არსებითი სახელი",
  
)


mutate(
  det = case_when(
    str_detect(meta, "არსებითი სახელი") ~ "noun",
    str_detect(meta, "ზედსართავი სახელი") ~ "adjective",    
    str_detect(meta, "ზმნიზედა") ~ "adverb",
    str_detect(meta, "ნაცვალსახელი") ~ "pronoun", # местоимения
    str_detect(meta, "ზმნა") ~ "verb",
    str_detect(meta, "კავშირი") ~ "conjunction", # союзы
    str_detect(meta, "რიცხვითი სახელი") ~ "number",
    str_detect(meta, "თანდებული") ~ "suffix",  # послеслоги
    str_detect(meta, "ნაწილაკი") ~ "particle",  # отрицания и другие частицы
    str_detect(meta, "ნათესაობითი ბრუნვა") ~ "n/a",
    T ~ "n/a"
  )
)
