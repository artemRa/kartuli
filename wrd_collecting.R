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

# saveRDS(conjugation, "conjugation.RData")


conjugation$meta %>% 
  filter(infinitive != "") %>%
  filter(str_detect(infinitive, "/")) %>%
  view()

# lost infinitive
conjugation$meta %>% 
  filter(infinitive != "") %>%
  mutate(
    preverb = if_else(
      lid %in% c(28, 85, 92, 153, 202, 214) | str_length(preverb) > 4,
      "",
      preverb
    )
  ) %>% 
  mutate(
    infinitive2 = str_extract(infinitive, "(?<=/).*"),
    infinitive = str_extract(infinitive, "^[^/]*"),
    infinitive = if_else(
      preverb != "" & !str_detect(infinitive, paste0("^", preverb)),
      paste0(preverb, infinitive),
      infinitive
    )
  ) %>% 
  rename(wrd = infinitive) %>%
  distinct(wrd) %>% 
  anti_join(raw_ka_words, by = "wrd") %>%
  dbAppendTable(conn, "ka_words_sample", .)

filter(ka_word_tidy_dict, pos == "verb", tid == "X000") %>% 
  select(wid, word, eng) %>% 
  group_by(wid) %>% 
  filter(n() > 1) %>% 
  arrange(wid) %>% 
  view()
  

infinitve_full_info <- conjugation$meta %>% 
  filter(infinitive != "") %>%
  mutate(
    preverb = if_else(
      lid %in% c(28, 85, 92, 153, 202, 214) | str_length(preverb) > 4,
      "",
      preverb
    )
  ) %>% 
  mutate(
    infinitive = str_extract(infinitive, "^[^/]*"),
    infinitive = if_else(
      preverb != "" & !str_detect(infinitive, paste0("^", preverb)) & str_length(preverb) < 5,
      paste0(preverb, infinitive),
      infinitive
    )
  ) %>% 
  inner_join(raw_ka_words, by = c("infinitive" = "wrd")) %>% 
  left_join(
    filter(ka_word_tidy_dict, pos == "verb", tid == "X000") %>% 
      select(wid, eng), 
    by = "wid"
  ) %>% 
  distinct(wid, infinitive, english, eng) %>% 
  nest(data = -c(wid, infinitive)) %>% 
  mutate(
    eng0 = map_chr(data, ~ paste(glue_data(., "{english}"), collapse = " / ")),
    eng1 = map_chr(data, ~ unique(.x$eng)),
    eng = if_else(str_detect(eng0, eng1) | is.na(eng1), eng0, paste(eng1, "/", eng0))
    ) %>%
  select(wid, word = infinitive, eng)
  
eng_form_data <- infinitve_full_info %>% 
  inner_join(
    filter(ka_word_tidy_dict, pos == "verb", tid == "X000") %>% 
      select(wid), 
    by = "wid"
  )

dbExecute(
  conn, 
  "UPDATE ka_word_tidy_dict SET eng = ? WHERE wid = ? and tid = 'X000' and pos = 'verb'", 
  params = list(eng_form_data$eng, eng_form_data$wid)
)


dbExecute(conn, "UPDATE ka_word_tidy_dict SET num = 2 WHERE wid = 2559")
infinitve_full_info %>% 
  anti_join(
    filter(ka_word_tidy_dict, pos == "verb", tid == "X000") %>% 
      select(wid), 
    by = "wid"
  ) %>%
  mutate(oid = wid, pos = "verb", tid = "X000", num = 1L, source = "L2") %>% 
  select(wid, num, pos, tid, oid, word, source, eng) %>%
  dbAppendTable(conn, "ka_word_tidy_dict", .)

infinitive_clones <- conjugation$meta %>% 
  filter(infinitive != "") %>%
  filter(str_detect(infinitive, "/")) %>% 
  mutate(
    preverb = if_else(
      lid %in% c(28, 85, 92, 153, 202, 214) | str_length(preverb) > 4,
      "",
      preverb
    )
  ) %>% 
  mutate(
    infinitive2 = str_extract(infinitive, "(?<=/).*"),
    infinitive = str_extract(infinitive, "^[^/]*"),
    infinitive = if_else(
      preverb != "" & !str_detect(infinitive, paste0("^", preverb)),
      paste0(preverb, infinitive),
      infinitive
    )
  ) %>% 
  select(wrd = infinitive, clone = infinitive2) %>% 
  inner_join(raw_ka_words, by = "wrd") %>% 
  select(oid = wid, clone)
  
infinitive_clones %>% 
  anti_join(ka_word_tidy_dict, by = c("clone" = "word")) %>% 
  inner_join(raw_ka_words, by =  c("clone" = "wrd")) %>% 
  mutate(pos = "verb", tid = "X001", num = 1L, source = "L2") %>% 
  select(wid, num, pos, tid, oid, word = clone, source) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)


conjugation_connector <- conjugation$meta %>% 
  filter(infinitive != "") %>%
  mutate(
    preverb = if_else(
      lid %in% c(28, 85, 92, 153, 202, 214) | str_length(preverb) > 4,
      "",
      preverb
    )
  ) %>% 
  mutate(
    infinitive = str_extract(infinitive, "^[^/]*"),
    infinitive = if_else(
      preverb != "" & !str_detect(infinitive, paste0("^", preverb)) & str_length(preverb) < 5,
      paste0(preverb, infinitive),
      infinitive
    )
  ) %>% 
  select(lid, infinitive) %>% 
  inner_join(infinitve_full_info, by = c("infinitive" = "word")) %>% 
  rename(oid = wid) %>% 
  select(oid, lid)


verb_form_dict <- merge(verb_tense_data, verb_forms_data) %>% 
  mutate(tid = paste0("V", sprintf("%02d", num), id)) %>% 
  select(tid, tense, numb, prsn) %>% 
  arrange(tid)

conjugation_all_verbs <- conjugation$verbs %>%
  inner_join(conjugation_connector, by = "lid") %>% 
  select(-lid) %>% 
  distinct() %>% 
  separate(word, into = c("word1", "word2"), sep = "/", fill = "right") %>% 
  pivot_longer(cols = starts_with("word"), values_to = "word") %>%
  filter(!is.na(word)) %>%
  select(-name) %>% 
  mutate(
    word1 = str_remove(word, ".*?\\)"),
    word2 = str_remove_all(word, "[[:punct:][:space:]]")
  ) %>% 
  mutate_at(vars(starts_with("word")), str_squish) %>% 
  mutate(words2 = if_else(word1 == word2, as.character(NA), word2)) %>% 
  select(-word) %>% 
  pivot_longer(cols = starts_with("word"), values_to = "word") %>%
  filter(!is.na(word)) %>%
  select(-name) %>% 
  mutate(
    word = str_remove_all(word, "[[:punct:]]"),
    word = str_remove(word, " .*")
  ) %>% 
  inner_join(verb_form_dict, by = c("tense", "numb", "prsn")) %>% 
  distinct() %>%
  select(oid, word, tid)
  

conjugation_all_verbs %>% 
  anti_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  select(wrd = word) %>% 
  dbAppendTable(conn, "ka_words_sample", .)


mnum_df <- ka_word_tidy_dict %>% group_by(wid) %>% summarise(mnum = max(num))
conjugation_all_verbs %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  mutate(pos = "verb", source = "L2") %>% 
  anti_join(ka_word_tidy_dict, by = c("oid", "wid", "tid", "pos")) %>% 
  left_join(mnum_df, by = "wid") %>% 
  group_by(word) %>% 
  mutate(num = row_number(paste0(sprintf("%09d", oid), tid)) + coalesce(mnum, 0L)) %>% 
  ungroup() %>% 
  select(wid, num, pos, tid, oid, word, source) %>%
  dbAppendTable(conn, "ka_word_tidy_dict", .)


all_participle_data <- conjugation$meta %>% 
  select(lid, participle) %>% 
  rename(word = participle) %>%
  separate(word, into = c("word1", "word2"), sep = "/", fill = "right") %>% 
  pivot_longer(cols = starts_with("word"), values_to = "word") %>%
  filter(!is.na(word)) %>%
  select(-name) %>% 
  mutate(
    word1 = str_remove(word, ".*?\\)"),
    word2 = str_remove_all(word, "[[:punct:][:space:]]")
  ) %>% 
  mutate_at(vars(starts_with("word")), str_squish) %>% 
  mutate(words2 = if_else(word1 == word2, as.character(NA), word2)) %>% 
  select(-word) %>% 
  pivot_longer(cols = starts_with("word"), values_to = "word") %>%
  filter(!is.na(word)) %>%
  inner_join(conjugation_connector, by = "lid") %>% 
  select(-c(name, lid)) %>% 
  filter(word != "") %>% 
  mutate(
    word = str_remove_all(word, "[[:punct:]]"),
    word = str_remove(word, " .*")
  ) %>% 
  filter(str_length(word) > 5) %>% 
  distinct()

all_participle_data %>% 
  anti_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  select(wrd = word) %>% 
  dbAppendTable(conn, "ka_words_sample", .)

mnum_df <- ka_word_tidy_dict %>% group_by(wid) %>% summarise(mnum = max(num))
all_participle_data %>% 
  filter(word != "") %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  mutate(pos = "verb", tid = "V001", source = "L2") %>% 
  anti_join(ka_word_tidy_dict, by = c("oid", "wid", "tid", "pos")) %>% 
  left_join(mnum_df, by = "wid") %>% 
  group_by(word) %>% 
  mutate(num = row_number(oid) + coalesce(mnum, 0L)) %>% 
  ungroup() %>% 
  select(wid, num, pos, tid, oid, word, source) %>%
  dbAppendTable(conn, "ka_word_tidy_dict", .)

dbExecute(conn, "DELETE FROM ka_word_tidy_dict WHERE word = ''")
dbExecute(conn, "DELETE FROM ka_words_sample WHERE wrd = ''")
dbExecute(conn, "UPDATE ka_word_tidy_dict SET eng = NULL WHERE tid = 'V001' and pos = 'verb'")


load("ganmarteba_words_list.RData")
load("ganmarteba_extra_list.RData")


# words from dictionary
ganmarteba_words <- 
  ganmarteba_words_list %>% 
  reduce(add_row) %>% 
  mutate(word = str_remove(word, "[[:digit:]]$"))

# meaning and examples
ganmarteba_extra <- 
  ganmarteba_extra_list %>% 
  reduce(add_row) %>% 
  mutate(meaning = str_replace_all(meaning, "\\([[:digit:]]\\)", " "))

dbAppendTable(conn, "ka_ganmarteba_meta", ganmarteba_words)
dbAppendTable(conn, "ka_ganmarteba_examples", ganmarteba_extra)


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

ganmarteba_word_collector <- function(bunch_of_words) {
  
  pb <- progress_bar$new(
    format = "[:bar] :percent ETA: :eta",
    total = nrow(bunch_of_words)
  )
  
  ganmarteba_words_list <- list()
  ganmarteba_extra_list <- list()
  k <- 1L
  
  for (i in bunch_of_words$wid) {
    
    wrd <- filter(bunch_of_words, wid == !!i) %>% pull(wrd)
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
            wid  = i,
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
  meta <- 
    ganmarteba_words_list %>% 
    reduce(add_row) %>% 
    mutate(word = str_remove(word, "[[:digit:]]$"))
  
  # meaning and examples
  extra <- 
    ganmarteba_extra_list %>% 
    reduce(add_row) %>% 
    mutate(meaning = str_replace_all(meaning, "\\([[:digit:]]\\)", " "))
  
  
  list(words = meta, extra = extra)
  
}

verb_words <- ka_word_tidy_dict %>% 
  filter(pos == "verb", tid %in% c("X000", "X001", "V001"), is.na(desc)) %>% 
  select(wid, wrd = word, tid, oid) %>% 
  distinct()

ganmarteba_new <- ganmarteba_word_collector(verb_words)
pos_index <- map_int(ganmarteba_new$words$pos0, ~ which(str_detect(.x, part_of_speach_dict$geo))[1])
gan_words_cleared <- ganmarteba_new$words %>% 
  mutate(pos = !!part_of_speach_dict[pos_index,]$eng, .before = "pos0") %>% 
  mutate_at(vars(pos0, pos), ~if_else(is.na(pos), as.character(NA), .x)) %>% 
  mutate(pos = coalesce(pos, "oth")) %>% 
  mutate(pos = if_else(pos0 == "არსებითი სახელი (საწყისი)", "verb", pos))  # отглагольное существ.


extra_verb_forms <- verb_words %>% 
  filter(tid %in% c("X000", "X001")) %>% 
  inner_join(
    filter(gan_words_cleared, pos == "verb"),
    by = "wid"
  ) %>% 
  left_join(ganmarteba_new$extra,  by = "gid") %>% 
  mutate(meaning = str_squish(meaning)) %>% 
  filter(!str_detect(meaning, "[[:space:]]")) %>%
  select(oid, word = meaning) %>% 
  mutate(word = str_remove_all(word, "[[:punct:][:digit:]]"))

extra_verb_forms %>% 
  anti_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  select(wrd = word) %>% 
  dbAppendTable(conn, "ka_words_sample", .)

mnum_df <- ka_word_tidy_dict %>% group_by(wid) %>% summarise(mnum = max(num))
extra_verb_forms %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>%
  left_join(mnum_df, by = "wid") %>% 
  group_by(word) %>% 
  mutate(num = row_number(oid) + coalesce(mnum, 0L)) %>% 
  mutate(pos = "verb", source = "G2", tid = "X001") %>% 
  select(wid, num, pos, tid, oid, word, source) %>%
  dbAppendTable(conn, "ka_word_tidy_dict", .) 

verb_meaning <- verb_words %>%
  filter(tid %in% c("X000", "X001")) %>% 
  inner_join(
    filter(gan_words_cleared, pos == "verb"),
    by = "wid"
  ) %>% 
  left_join(ganmarteba_new$extra,  by = "gid") %>% 
  mutate(meaning = str_squish(meaning)) %>% 
  mutate(bad_desc = if_else(str_detect(meaning, "[[:space:]]"), 0L, 1L)) %>% 
  group_by(wid) %>% 
  arrange(wid, bad_desc, site, mid) %>%
  filter(n() > 1) %>%
  slice(1L) %>% 
  ungroup() %>% 
  select(oid, wid, tid, desc = meaning)

dbExecute(
  conn, 
  "UPDATE ka_word_tidy_dict SET desc = ? WHERE oid = ? and wid = ? and tid = ? and pos = 'verb'", 
  params = list(verb_meaning$desc, verb_meaning$oid, verb_meaning$wid, verb_meaning$tid)
)




raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")

