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
verb_tense_data <- gsheet::gsheet2tbl(config$glink1)
verb_forms_data <- gsheet::gsheet2tbl(config$glink3)
extra_word_forms <- gsheet::gsheet2tbl(config$glink2)
verified_ending <- gsheet::gsheet2tbl(config$glink4)

# export pre-clearing Georgian words 
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")

# temporary tables
# dbWriteTable(conn, "ganmarteba_words", ganmarteba_words_ext)
# dbWriteTable(conn, "ganmarteba_extra", ganmarteba_extra)
# dbWriteTable(conn, "conjugate_words", conjugate_words)
# dbWriteTable(conn, "conjugate_forms", conjugate_forms)
# ganmarteba_words <- dbReadTable(conn, "ganmarteba_words")
# ganmarteba_extra <- dbReadTable(conn, "ganmarteba_extra")
# conjugate_words <- dbReadTable(conn, "conjugate_words")
# conjugate_forms <- dbReadTable(conn, "conjugate_forms")


# read html ignoring errors
read_html_iter <- function(web_link, max_attempt = 10, ...) {
  for (i in 1:max_attempt) {
    export_html <- try(xml2::read_html(web_link, ...), silent = TRUE)
    if (class(export_html)[1] != "try-error") break
  }

  export_html
}


# Extra word information ----
# 1. Main countries names
# W1: ქვეყნების სია
kvek_link <- "https://ka.wikipedia.org/wiki/ქვეყნების_სია"
kvek_html <- read_html_iter(kvek_link)
kvek_tbls <- html_nodes(kvek_html, "table")

tb1 <- html_table(kvek_tbls[[2]])
for (i in 3:29) {
  tb2 <- html_table(kvek_tbls[[i]])
  tb1 <- add_row(tb1, tb2)
}

country_word_table <- tb1 %>%
  set_names("id", "short_name", "full_name", "capital") %>% 
  select(-id) %>%
  mutate_all(~if_else(.x == "—", as.character(NA), .x)) %>% 
  mutate(id = row_number(), .before = 1L)

country_name_table <- country_word_table %>%
  mutate(relative = str_extract(full_name, "\\w+")) %>%
  mutate(relative = if_else(
    str_detect(relative, "ს$") & !str_detect(short_name, relative) & str_sub(relative, 1, 1) == str_sub(short_name, 1, 1), 
    relative, as.character(NA))) %>% 
  filter(!str_detect(short_name, "[[:space:]]")) %>% 
  select(word = short_name, relative, desc = full_name)

# adding lost countries
country_name_table %>% 
  select(word, relative) %>% 
  pivot_longer(cols = everything(), values_drop_na = T, values_to = "wrd") %>% 
  select(-name) %>%
  anti_join(raw_ka_words, by = "wrd") %>% 
  dbAppendTable(conn, "ka_words_sample", .)

# adding lost capitals
country_word_table %>% 
  filter(!is.na(capital)) %>% 
  anti_join(raw_ka_words, by = c("capital" = "wrd")) %>% 
  select(wrd = capital) %>% 
  dbAppendTable(conn, "ka_words_sample", .)

raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")

# countries names
country_name_table %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  mutate(pos = "noun", num = 1L, tid = "X000", source = "W1", oid = wid) %>% 
  select(wid, num, pos, oid, tid, word, desc, source) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)

# countries relatives
country_name_table %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  rename(oid = wid) %>% 
  inner_join(raw_ka_words, by = c("relative" = "wrd")) %>% 
  mutate(pos = "noun", num = 1L, tid = "N000", source = "W1") %>% 
  select(wid, num, pos, oid, tid, word = relative, source) %>%
  dbAppendTable(conn, "ka_word_tidy_dict", .)

# manual fixing: gana & america
dbExecute(conn, "UPDATE ka_word_tidy_dict SET num = ? WHERE wid = ?", params = list(2L, 401L))
filter(raw_ka_words, wrd == "ამერიკა") %>% 
  mutate(oid = 3560L, pos = "noun", num = 1L, tid = "X001", word = wrd, source = "W1") %>% 
  select(wid, num, pos, oid, tid, word, source) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)

country_word_table %>% 
  filter(!is.na(capital)) %>% 
  inner_join(raw_ka_words, by = c("capital" = "wrd")) %>% 
  mutate(pos = "noun", tid = "X000", source = "W1", oid = wid) %>% 
  mutate(num = if_else(capital == short_name, 2L, 1L)) %>% 
  mutate(desc = paste("დედაქალაქი:", full_name)) %>% 
  select(wid, num, pos, oid, tid, word = capital, source, desc) %>%
  dbAppendTable(conn, "ka_word_tidy_dict", .)

# manual fixing: male & names
dbExecute(conn, "UPDATE ka_word_tidy_dict SET num = 2 WHERE wid = ?", params = list(c(170, 4163, 4432)))


# 2. Main languages names
# W2: მსოფლიოს ენები
ena_link <- "https://www.nplg.gov.ge/wikidict/index.php/მსოფლიოს_ენები"
ena_html <- read_html_iter(ena_link)

ena_descr <- ena_html %>% 
  html_nodes("p") %>% 
  html_text2() %>% 
  str_split("\n") %>%
  unlist() %>% 
  .[str_detect(., "^([[:digit:]])([[:digit:]]|)\\.( )[ა-ჰ]")] %>% 
  str_remove("^([[:digit:]])([[:digit:]]|)\\.") %>% 
  str_squish()
 
ena1 <- ena_descr %>% 
  str_remove("^(ძველი|ახალი|საშუალო|ენა|ვულგარული)") %>% 
  str_squish() %>% 
  str_remove("([[:punct:]]| )(.*)$")
  
ena2 <- ena_descr %>% 
  str_extract("(ანუ)( )(.*)") %>% 
  str_remove("(ანუ)( )") %>% 
  str_squish() %>% 
  str_remove("^(ძველი|ახალი|საშუალო|ენა|ვულგარული)") %>% 
  str_squish() %>% 
  str_remove("([[:punct:]]| )(.*)$")

language_word_table <- 
  tibble(word = c(ena1, ena2), descr = rep(ena_descr, 2)) %>% 
  filter(!is.na(word)) %>% 
  group_by(word) %>% 
  filter(row_number() == 1L)

language_word_table %>% 
  anti_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  select(wrd = word)  %>% 
  dbAppendTable(conn, "ka_words_sample", .)

raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")

language_word_table %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  anti_join(ka_word_tidy_dict, by = "wid") %>% 
  mutate(num = 1L, oid = wid, tid = "X000", source = "W2") %>% 
  mutate(pos = if_else(str_detect(word, "უ[რლ]ი$"), "adj", "noun")) %>% 
  mutate_at(vars(descr), str_remove, pattern = "[[:punct:]]$") %>% 
  select(wid, num, pos, oid, tid, word, source, desc = descr) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)


# 3. Biggest cities
# W3: მსოფლიოს უდიდესი ქალაქები

city_link <- "https://ka.wikipedia.org/wiki/მსოფლიოს_უდიდესი_ქალაქები"
city_html <- read_html_iter(city_link)
city_data <- html_nodes(city_html, "table")[[1]] %>% 
  html_table()

city_table <- city_data[, c("რანგი", "ქალაქი","ქვეყანა")] %>% 
  set_names(c("id", "city", "country"))

city_table %>% 
  anti_join(raw_ka_words, by = c("city" = "wrd")) %>% 
  select(wrd = city)  %>% 
  dbAppendTable(conn, "ka_words_sample", .)

raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")

city_table %>% 
  inner_join(raw_ka_words, by = c("city" = "wrd")) %>% 
  anti_join(ka_word_tidy_dict, by = "wid") %>% 
  mutate(num = 1L, pos = "noun", oid = wid, tid = "X000", source = "W3") %>% 
  mutate(desc = paste("ქალაქი:", country)) %>% 
  select(wid, num, pos, oid, tid, word = city, source, desc) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)
  

  
# Export data from Ganmarteba dictionary ----
# G1: https://www.ganmarteba.ge/
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

top_words <- raw_ka_words %>% 
  arrange(wid) %>% 
  head(5000L)

pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = nrow(top_words)
)

ganmarteba_words_list <- list()
ganmarteba_extra_list <- list()
k <- 1L

for (i in top_words$wid) {

  wrd <- filter(top_words, wid == !!i) %>% pull(wrd)
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

save(ganmarteba_words_list, file = "ganmarteba_words_list.RData")
save(ganmarteba_extra_list, file = "ganmarteba_extra_list.RData")

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
  mutate(pos = !!part_of_speach_dict[pos_index,]$eng, .before = "pos0") %>% 
  mutate_at(vars(pos0, pos), ~if_else(is.na(pos), as.character(NA), .x)) %>% 
  mutate(pos = coalesce(pos, "oth")) %>% 
  mutate(pos = if_else(pos0 == "არსებითი სახელი (საწყისი)", "verb", pos))  # отглагольное существ.

cleared_ganmarteba_words <- ganmarteba_words_ext %>% 
  inner_join(select(raw_ka_words, wid, wrd), by = c("wid", "word" = "wrd")) %>%
  mutate(dig = max(coalesce(as.integer(str_extract(site, "[[:digit:]]")), 9L))) %>%
  group_by(word) %>%
  mutate(num = dense_rank(desc(paste0(dig, pos)))) %>%
  ungroup() %>%
  group_by(word, pos) %>% 
  mutate(rn = row_number(dig)) %>% 
  select(-dig)

cleared_ganmarteba_words %>% 
  left_join(ganmarteba_extra, by = "gid") %>%
  filter(rn == 1L, coalesce(mid, 1L) == 1L) %>% 
  mutate(tid = "X000", source = "G1", oid = wid, desc = meaning) %>% 
  select(wid, num, pos, tid, oid, word, desc, source) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)

relative_cleared_ganmarteba_words <- 
  cleared_ganmarteba_words %>% 
  filter(!is.na(relative)) %>% 
  mutate(data = map(relative, ~ str_split(.x, " "))) %>%
  unnest(data) %>% 
  mutate(data = map(data, ~ tibble(data = .x))) %>%
  unnest(data) %>% 
  mutate_at(vars(data), ~ str_remove(.x, "[[:punct:]]$")) %>% 
  rename(wrd = data) %>% 
  filter(rn == 1L) %>% 
  group_by(wrd) %>% 
  select(-c(rn, num)) %>% 
  mutate(num = row_number(desc(wid))) %>% 
  ungroup()
  
relative_cleared_ganmarteba_words %>% 
  anti_join(raw_ka_words, by = "wrd") %>% 
  distinct(wrd) %>% 
  dbAppendTable(conn, "ka_words_sample", .)

raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")
ka_word_tidy_dict_max <- ka_word_tidy_dict %>% 
  group_by(wid) %>% 
  summarise(numm = max(num))

relative_cleared_ganmarteba_words %>% 
  rename(oid = wid) %>% 
  inner_join(raw_ka_words, by = "wrd") %>% 
  left_join(ka_word_tidy_dict_max, by = "wid") %>% 
  mutate(tid = "N000", source = "G1") %>% 
  mutate(desc = paste(word, "(ნათესაობითი ბრუნვა)")) %>% 
  mutate(num = coalesce(numm, 0L) + num) %>% 
  select(-word) %>% 
  select(wid, num, pos, tid, oid, word = wrd, desc, source) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)

ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")
top_words %>% 
  left_join(filter(ka_word_tidy_dict, num == 1L), by = "wid") %>% 
  mutate(gr = ntile(wid, 5)) %>% 
  group_by(gr) %>% 
  summarise(coverage = sum(num, na.rm = T) / n()) %>% 
  mutate(x = 1, y = gr * 1000) %>% 
  ggplot(aes(x = x, y = y, fill = coverage)) +
  geom_tile(show.legend = F) +
  geom_text(aes(label = scales::percent_format(accuracy = 1L)(coverage))) +
  scale_fill_gradient(limits = c(0, 1), low = "yellow", high = "red") +
  labs(
    x = NULL, y = NULL, 
    title = "Top 5K words coverage",
    subtitle = "www.ganmarteba.ge"
  ) +
  theme(axis.text.x = element_blank())



# Export verbs from Lingua.ge ----
# L1: https://lingua.ge/verbs

# Export from google-search
verb_query <- "site:https://lingua.ge/verbs"
page_index <- 10L*(0L:70L)
verb_links_list <- list()
j <- 1L
for (i in page_index) {
  google_url <- paste0("https://www.google.com/search?q=", URLencode(verb_query), "&start=", i)
  google_page <- read_html_iter(google_url)
  verb_links_list[[j]] <- google_page %>%
    html_nodes("a") %>%
    html_attr("href") %>%
    grep("^/url\\?q=", ., value = TRUE) %>%
    gsub("^/url\\?q=", "", .) %>%
    gsub("&.*", "", .) %>% 
    map_chr(~ iconv(URLdecode(.x), "UTF-8", "UTF-8")) %>% 
    map_chr(~ iconv(URLdecode(.x), "UTF-8", "UTF-8")) %>% 
    grep("https://lingua.ge/verbs/", ., value = TRUE)
  j <- j+1L
}

# Getting verbs information
banch_of_verb_link <- verb_links_list %>% reduce(c)

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
      filter(meta %in% c("English", "Infinitive", "Preverb", "Participle")) %>% 
      pivot_wider(names_from = meta, values_from = info) %>% 
      rename_all(tolower) %>% 
      # filter(info != "") %>% 
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
    num_form_vector2 <- c(129) # , 137, 145)
    
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

# saveRDS(conjugate_meta, "conjugate_meta.RData")
# saveRDS(conjugate_verbs, "conjugate_verbs.RData")

verb_form_dict <- merge(verb_tense_data, verb_forms_data) %>% 
  mutate(tid = paste0("V", sprintf("%02d", num), id)) %>% 
  select(tid, tense, numb, prsn) %>% 
  arrange(tid)

conjugate_verbs_df <- 
  conjugate_verbs %>% 
  reduce(add_row)

conjugate_meta_df <- 
  conjugate_meta %>% 
  reduce(add_row)

# adding lost infinitve
conjugate_meta_df %>% 
  filter(infinitive != "") %>% 
  anti_join(raw_ka_words, by = c("infinitive" = "wrd")) %>% 
  select(wrd = infinitive) %>% 
  dbAppendTable(conn, "ka_words_sample", .)
  
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
eng_form_data <- conjugate_meta_df %>% 
  filter(infinitive != "") %>% 
  inner_join(raw_ka_words, by = c("infinitive" = "wrd")) %>% 
  inner_join(ka_word_tidy_dict, by = "wid") %>%
  filter(pos == "verb", tid == "X000", num == 1L) %>% 
  select(wid, english)

dbExecute(conn, "UPDATE ka_word_tidy_dict SET eng = ? WHERE wid = ? and tid = 'X000' and num = 1 and pos = 'verb'", params = list(eng_form_data$english, eng_form_data$wid))

mnum_df <- ka_word_tidy_dict %>% 
  group_by(wid) %>% 
  summarise(mnum = max(num))

conjugate_meta_df %>% 
  filter(infinitive != "") %>% 
  inner_join(raw_ka_words, by = c("infinitive" = "wrd")) %>% 
  anti_join(filter(ka_word_tidy_dict, pos == "verb", tid == "X000"), by = "wid") %>% 
  left_join(mnum_df, by = "wid") %>% 
  mutate(tid = "X000", source = "L1", pos = "verb", oid = wid, eng = english, num = coalesce(mnum, 0L) + 1L, word = infinitive) %>% 
  select(wid, num, pos, tid, oid, word, source, eng) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)

conjugate_verbs_df %>% 
  filter(!str_detect(word, "[[:space:]]")) %>% 
  filter(str_detect(word, "[^ა-ჰ]")) %>% 
  distinct(word) %>% 
  view()


unnest_verbs <- conjugate_verbs_df %>% 
  filter(!str_detect(word, "[[:space:]]")) %>% 
  filter(str_detect(word, "\\/")) %>% 
  mutate(word = map(word, ~ str_split(.x, "/"))) %>%
  unnest(word) %>%
  mutate(word = map(word, ~ tibble(word = .x))) %>%
  unnest(word) %>% 
  mutate_at(vars(word), str_squish) %>% 
  filter(word != "")

bracket_verbs <- conjugate_verbs_df %>% 
  filter(!str_detect(word, "[[:space:]]")) %>% 
  filter(str_detect(word, "\\)[ა-ჰ]"))

cleared_verb_forms <- conjugate_verbs_df %>% 
  filter(!str_detect(word, "[[:space:]]")) %>% 
  mutate_at(vars(word), str_remove_all, pattern = "\\*") %>% 
  filter(!str_detect(word, "[^ა-ჰ]")) %>% 
  add_row(mutate_at(bracket_verbs, vars(word), str_remove_all, pattern = "[[:punct:]]")) %>% 
  add_row(mutate_at(bracket_verbs, vars(word), str_remove_all, pattern = "\\(.*\\)")) %>% 
  add_row(unnest_verbs) %>% 
  distinct()
  
cleared_verb_forms %>% 
  distinct(word) %>% 
  anti_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  select(wrd = word) %>%
  dbAppendTable(conn, "ka_words_sample", .)

raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")
mnum_df <- ka_word_tidy_dict %>% group_by(wid) %>% summarise(mnum = max(num))
origin_df <- conjugate_meta_df %>% 
  filter(infinitive != "") %>% 
  inner_join(raw_ka_words, by = c("infinitive" = "wrd")) %>%
  select(lid, wid)


cleared_verb_forms %>% 
  inner_join(verb_form_dict, by = c("tense", "numb", "prsn")) %>% 
  inner_join(origin_df, by = "lid") %>% 
  rename(oid = wid) %>%
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  left_join(mnum_df, by = "wid") %>% 
  group_by(wid) %>% 
  mutate(num = row_number(tid) + coalesce(mnum, 0L)) %>% 
  ungroup() %>% 
  mutate(source = "L1", pos = "verb") %>% 
  select(wid, num, pos, tid, oid, word, source) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)

participle_df <- conjugate_meta_df %>% 
  filter(participle != "") %>% 
  select(word = participle, infinitive, english) %>% 
  mutate(word = map(word, ~ str_split(.x, "/"))) %>%
  unnest(word) %>%
  mutate(word = map(word, ~ tibble(word = .x))) %>%
  unnest(word) %>% 
  mutate_at(vars(word), str_squish) %>% 
  mutate_at(vars(word), str_remove_all, pattern = "\\*")

bracket_participle_df <- participle_df %>% 
  filter(str_detect(word, "\\)[ა-ჰ]"))

participle_df_clear <- participle_df %>% 
  filter(!str_detect(word, "[^ა-ჰ]")) %>% 
  add_row(mutate_at(bracket_participle_df, vars(word), str_remove_all, pattern = "[[:punct:]]")) %>% 
  add_row(mutate_at(bracket_participle_df, vars(word), str_remove_all, pattern = "\\(.*\\)")) %>% 
  filter(infinitive != "") %>% 
  inner_join(select(raw_ka_words, wrd, wid), by = c("infinitive" = "wrd")) %>% 
  rename(oid = wid) %>% 
  mutate_at(vars(english), str_remove, pattern = "( v.i$)|( v.t$)") %>% 
  mutate_at(vars(english), str_remove, pattern = "^to ") %>% 
  mutate_at(vars(english), ~ str_replace(.x, "([[:space:]])|($)", "\\(ing\\/d\\) ")) %>% 
  mutate_at(vars(english), str_squish) %>% 
  select(-infinitive) %>% 
  rename(eng = english)
  

participle_df_clear %>% 
  anti_join(raw_ka_words, by = c("word" = "wrd")) %>%
  distinct(wrd = word) %>% 
  dbAppendTable(conn, "ka_words_sample", .)

raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")
mnum_df <- ka_word_tidy_dict %>% group_by(wid) %>% summarise(mnum = max(num))
participle_df_clear %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  left_join(mnum_df, by = "wid") %>% 
  group_by(word) %>% 
  mutate(num = row_number(oid) + coalesce(mnum, 0L)) %>% 
  ungroup() %>% 
  mutate(pos = "verb", tid = "V001", source = "L1") %>% 
  select(wid, num, pos, tid, oid, word, source, eng) %>%
  dbAppendTable(conn, "ka_word_tidy_dict", .)
  

# Word building ----
# N1: custom code

ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")

relative_forms <- ka_word_tidy_dict %>% 
  filter(tid == "N000")

alternative_forms <- ka_word_tidy_dict %>% 
  filter(tid == "X001")

origin_forms_p1 <- ka_word_tidy_dict %>% 
  filter(oid == wid) %>% 
  filter(tid == "X000") %>% 
  anti_join(relative_forms, by = c("oid", "pos")) %>% 
  filter(pos %in% c("noun", "adj"))

origin_forms_p2 <- ka_word_tidy_dict %>% 
  filter(oid == wid) %>% 
  inner_join(select(relative_forms, oid, pos), by = c("oid", "pos")) %>% 
  filter(tid == "X000")

origin_forms <- add_row(origin_forms_p1, origin_forms_p2) %>% 
  group_by(wid) %>% filter(num == min(num)) %>% ungroup() %>% 
  arrange(wid)

origin_wid_list <- origin_forms$wid
unmatched_words <- raw_ka_words %>% anti_join(ka_word_tidy_dict, by = "wid")


word_list_temp0 <- list()
word_list_temp1 <- list()
word_list_temp2 <- list()
word_list1 <- list()
word_list2 <- list()
i <- 1L

aris <- c("ა", "აა", "აც", "ააც", "ც")
aris_ending0 <- paste0("(", aris, ")$", collapse = "|")
aris_ending1 <- tibble(tid = "N101", ending = !!aris) %>% add_row(tid = "N100", ending = "")
aris_ending2 <- tibble(tid = "N001", ending = !!aris)

pb <- progress_bar$new(
  format = "[:bar] :percent ETA: :eta",
  total = length(origin_wid_list)
)

for (wid in origin_wid_list) {
  
  wrd0 <- pull(filter(origin_forms, wid == !!wid), word)
  wrd1 <- pull(filter(relative_forms, oid == !!wid), word)
  plur <- nrow(filter(origin_forms, wid == !!wid, pos %in% c("noun", "adj"))) > 0
  
  plural_vector <- 
    c(
      paste0(wrd0, "ები"),
      paste0(str_remove(wrd0, "[იოუეა]$"), "ები"),
      paste0(str_remove(wrd1, "(ს$)|(ის$)"), "ები"),
      paste0(str_remove(wrd1, "(ს$)|([იოუეა]ს$)"), "ები")
    ) %>% 
    unique()
  
  root_vector <- 
    c(
      wrd0,
      str_remove(wrd0, "ი$"),
      str_remove(wrd0, "[იოუეა]$"),
      str_remove(wrd1, "(ს$)|(ის$)"),
      str_remove(wrd1, "(ს$)|([იოუეა]ს$)")
    ) %>% 
    unique()
  
  # Plural forms
  if (plur) {
    
    for (j in 1:length(plural_vector)) {
      
      word_list_temp0[[j]] <- tibble(wrd = plural_vector[j]) %>% 
        merge(aris_ending1) %>%
        mutate(wrd = paste0(wrd, coalesce(ending, ""))) %>% 
        inner_join(select(unmatched_words, wid, wrd), by = "wrd") %>% 
        select(-ending) %>% 
        distinct()
      
      plroot <- str_remove(plural_vector[j], ".$")
      word_list_temp1[[j]] <- unmatched_words %>% select(wid, wrd) %>%
        anti_join(word_list_temp0[[j]], by = "wid") %>% 
        filter(str_detect(wrd, paste0("^", plroot))) %>%
        mutate(ending = str_remove(wrd, paste0("^", plroot))) %>%
        filter(str_length(ending) < 10L) %>%
        mutate(., ending = str_remove(ending, !!aris_ending0))  %>% 
        inner_join(verified_ending, by = "ending") %>%
        select(-c(ending, name)) %>% distinct() %>% 
        mutate_at(vars(tid), str_replace, pattern = "^N0", replacement = "N1")
    }
    
    trusted_word_num <- which.max(map_int(word_list_temp1, nrow))[1]
    word_list1[[i]] <- rbind(word_list_temp0[[trusted_word_num]], word_list_temp1[[trusted_word_num]]) %>% 
      add_column(oid = !!wid, .before = 1L)
    unmatched_words <- unmatched_words %>% anti_join(word_list1[[i]], by = "wid")
    
    word_list_temp0 <- list()
    word_list_temp1 <- list()
  }
  
  # Singular forms
  word_list_temp2[[1]] <- tibble(wrd = !!wrd0) %>% 
    merge(aris_ending2) %>%
    mutate(wrd = paste0(wrd, coalesce(ending, ""))) %>% 
    inner_join(select(unmatched_words, wid, wrd), by = "wrd") %>% 
    select(-ending) %>% 
    distinct()
  
  for (j in 1:length(root_vector)) {
    
    sgroot <- root_vector[j]
    word_list_temp2[[j+1]] <- unmatched_words %>% select(wid, wrd) %>%
      anti_join(word_list_temp2[[1]], by = "wid") %>% 
      filter(str_detect(wrd, paste0("^", sgroot))) %>%
      mutate(ending = str_remove(wrd, paste0("^", sgroot))) %>%
      filter(str_length(ending) < 10L) %>%
      mutate(., ending = str_remove(ending, !!aris_ending0))  %>% 
      inner_join(verified_ending, by = "ending") %>%
      select(-c(ending, name)) %>% distinct() 
  }
  
  word_list2[[i]] <- reduce(word_list_temp2, rbind) %>% add_column(oid = !!wid, .before = 1L)
  unmatched_words <- unmatched_words %>% anti_join(word_list2[[i]], by = "wid")
  word_list_temp2 <- list()
  
  i <- i + 1
  pb$tick()
  
}

super_word_list <- rbind(reduce(word_list1, rbind), reduce(word_list2, rbind))
origin_forms_clear <- origin_forms %>% 
  group_by(wid) %>% 
  arrange(num) %>% 
  filter(row_number() == 1L) %>% 
  ungroup() %>% 
  select(wid, pos)

super_word_list %>% distinct() %>% 
  left_join(origin_forms_clear, by = c("oid" = "wid")) %>% 
  mutate(source = "N1", word = wrd, num = 1L) %>% 
  select(wid, num, pos, tid, oid, word, source) %>% 
  dbAppendTable(conn, "ka_word_tidy_dict", .)


