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


# Exporting data from Ganmarteba dictionary ----
# https://www.ganmarteba.ge/
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
  mutate_at(vars(pos0, pos1), ~if_else(is.na(pos1), as.character(NA), .x)) %>% 
  mutate(pos1 = if_else(pos0 == "არსებითი სახელი (საწყისი)", "vern", pos1)) # отглагольное существ.

# meaning and examples
ganmarteba_extra <- 
  ganmarteba_extra_list %>% 
  reduce(add_row) %>% 
  mutate(meaning = str_replace_all(meaning, "\\([[:digit:]]\\)", " "))


# dbWriteTable(conn, "ganmarteba_words", ganmarteba_words_ext)
# dbWriteTable(conn, "ganmarteba_extra", ganmarteba_extra)


# Exporting data from lingua.ge dictionary ----
# https://lingua.ge


top_words %>% 
  anti_join(ganmarteba_words, by = "id") %>% 
  view()

verbs_from_ganmarteba <- ganmarteba_words %>% 
  filter(!is.na(verbs)) %>% 
  pull(word)
  
pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = length(verbs_from_ganmarteba)
)

lingua_links <- list()
for (i in 1:length(verbs_from_ganmarteba)) {
  
  wrd <- verbs_from_ganmarteba[i]
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

banch_of_verb_link <- 
  lingua_links %>% 
  unlist() %>% 
  unique()


# wrd <- "გაცილება"
# web_link <- paste0("https://lingua.ge/verbs/", wrd)

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
    
    # web page parsing
    html_div_block <- html_elements(lingua_html, "div")
    html_div_class <- html_div_block %>% html_attr("class")
    verbs_position <- which(html_div_class == "elementor-widget-container")
    raw_div_vector <- html_div_block[verbs_position] %>% html_text2()
    
    div_start_line <- which(raw_div_vector == "Home » Conjugation")[1]
    clear_div_vector <- raw_div_vector[(div_start_line+1):length(raw_div_vector)]
    
    eng_translation <- str_remove(clear_div_vector[1], "(v.t.i.)|(v.i.)|(v.t.)") %>% str_squish()
    # wrd_origin <- clear_div_vector[4]
    conjugate_meta[[k]] <- 
      tibble(
        meta = clear_div_vector[2*(1:8)+1],
        info = clear_div_vector[2*(1:8)+2]
      ) %>% 
      rbind(c("English", eng_translation), .) %>% 
      filter(meta %in% c("English", "Infinitive", "Preverb")) %>% 
      pivot_wider(names_from = meta, values_from = info) %>% 
      rename_all(tolower) %>% 
      # filter(info != "") %>% 
      add_column(lid = !!k, .before = 1) %>% 
      mutate(link = !!banch_of_verb_link[i])
    
    num_form_vector1 <- c(2:4, 6:8)
    num_form_vector2 <- c(22, 31, 40, 51, 60, 69, 80, 89, 100, 109, 118)
    
    conjugate_tenses <- 
      map_df(num_form_vector2, 
             ~ tibble(
               word = clear_div_vector[num_form_vector1 + .x],
               form = c(rep(1, 3), rep(2, 3)),
               prsn = rep(1:3, 2),
               tens = str_remove_all(clear_div_vector[.x], "[ა-ჰ]") %>% str_squish(),
             )
      )
    
    num_form_vector1 <- c(2:3, 5:7)
    num_form_vector2 <- c(129, 137, 145)
    
    conjugate_imperative <- 
      map_df(num_form_vector2, 
             ~ tibble(
               word = clear_div_vector[num_form_vector1 + .x],
               form = c(rep(1, 2), rep(2, 3)),
               prsn = c(2:3, 1:3),
               tens = str_remove_all(clear_div_vector[.x], "[ა-ჰ]") %>% str_squish()
             )
      )
    
    conjugate_verbs[[k]] <- add_row(conjugate_tenses, conjugate_imperative) %>% 
      add_column(lid = !!k, .before = 1) %>% 
      filter(word != "")
   
    k <- k + 1 
  }
  pb$tick()
}

# words from dictionary
conjugate_words <- conjugate_meta %>% 
  reduce(add_row) %>% 
  filter(infinitive != "")

# verb forms
conjugate_forms <- conjugate_verbs %>% 
  reduce(add_row) %>% 
  filter(lid %in% conjugate_words$lid)
  

# dbWriteTable(conn, "conjugate_words", conjugate_words)
# dbWriteTable(conn, "conjugate_forms", conjugate_forms)

top_words %>% 
  anti_join(ganmarteba_words_ext, by = "id") %>% 
  select(id, wrd) %>% 
  inner_join(conjugate_forms, by = c("wrd" = "word")) %>% 
  mutate(tens2 = 
           case_when(
             tens == "Present indicative" ~ "Н",
             tens == "Imperfect" ~ "ПН",
             tens == "Aorist indicative" ~ "ПЗ",
             tens == "Future indicative" ~ "Б"
  )) %>% 
  filter(!is.na(tens2)) %>% 
  distinct(id, lid, wrd, tens2, form, prsn) %>% 
  inner_join(conjugate_words, by = "lid") %>% 
  view()


top_words %>% 
  anti_join(ganmarteba_words_ext, by = "id") %>% 
  select(id, wrd) %>% 
  anti_join(conjugate_forms, by = c("wrd" = "word")) %>% 
  view()





relative_words <- ganmarteba_words_ext %>%
  filter(pos1 == "noun") %>%
  filter(!is.na(relative)) %>% 
  select(id, gid, word, relative)

# ending_list <- list()
plural_list <-  list()
k <- 1

for (i in 1:nrow(relative_words)) {
  
  wrd0 <- relative_words$word[i]
  wrd1 <- relative_words$relative[i]
  root0 <- str_remove(wrd0, "[იოუეა]$")
  root1 <- str_remove(wrd1, "(ს$)|(ის$)")
  pl0 <- paste0(root0, "ები")
  pl1 <- paste0(root1, "ები")
  
  if (pl0 != pl1) {
    plural_list[[k]] <- raw_ka_words %>% 
      select(id, wrd) %>% 
      filter(wrd %in% c(pl0)) %>% 
      add_column(wrd0, wrd1, .after = 1L)
    k <- k + 1
  }
  
  # ending_list[[i]] <- raw_ka_words %>% 
  #   select(id, wrd) %>% 
  #   filter(str_detect(wrd, paste0("^", root))) %>% 
  #   filter(wrd != !!wrd0) %>% 
  #   mutate(ending = str_remove(wrd, paste0("^", root))) %>%
  #   filter(str_length(ending) < 10L) %>% 
  #   mutate(wrd0 = !!wrd0, wrd1 = !!wrd1)
}

plural_list %>% 
  reduce(add_row) %>% 
  view()

ending_frequency <- ending_list %>% 
  reduce(add_row) %>%
  group_by(ending) %>% 
  summarise(
    cnt = n(),
    wrd = max(paste(wrd0, wrd1, wrd)),
  ) %>% 
  filter(cnt > 1L) %>% 
  arrange(desc(cnt))
  

write.csv(ending_frequency, file = "ending_frequency.csv", fileEncoding = "UTF-8", row.names = FALSE)
readr::write_csv(ending_frequency, "ending_frequency.csv", col_names = TRUE)
