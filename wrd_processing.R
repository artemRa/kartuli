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
extra_word_forms <- gsheet::gsheet2tbl(config$glink2)

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



ganmarteba_words_changing <- ganmarteba_words_ext %>% 
  inner_join(select(raw_ka_words, wid, wrd), by = "wid") %>%
  filter(word != wrd) %>% view()
  select(-wrd)


ganmarteba_words_ext %>% 
  anti_join(ganmarteba_words_changing, by = "gid") %>%
  head() %>% 
  view()

ganmarteba_words_ext %>% 
  group_by(word, pos, num) %>% 
  filter(n() > 1) %>% 
  arrange(word, pos, num) %>% 
  view()


part_of_speach_priority <-
  c("excl", "conj", "part", "vern", "noun", "adv", "adj") %>% 
  tibble(part_of_speach = .) %>% 
  mutate(num = row_number(), .before = 1L)





# indirect match
ganmarteba_words_connect <- ganmarteba_words %>%
  inner_join(raw_ka_words, by = "id") %>% 
  filter(word != wrd) %>% 
  select_at(names(ganmarteba_words))

pos_index <- map_int(ganmarteba_words$pos0, ~ which(str_detect(.x, part_of_speach_dict$geo))[1])
ganmarteba_words_ext <- ganmarteba_words %>% 
  mutate(part_of_speach = !!part_of_speach_dict[pos_index,]$eng, .before = "pos0") %>% 
  mutate_at(vars(pos0, part_of_speach), ~if_else(is.na(part_of_speach), as.character(NA), .x)) %>% 
  mutate(part_of_speach = coalesce(part_of_speach, "other")) %>% 
  mutate(part_of_speach = if_else(pos0 == "არსებითი სახელი (საწყისი)", "vern", part_of_speach)) # отглагольное существ.
  
# original words dictionary
original_word_forms <- ganmarteba_words_ext %>% 
  anti_join(ganmarteba_words_connect, by = "gid") %>%
  inner_join(part_of_speach_priority, by = "part_of_speach") %>% 
  distinct(id, word, part_of_speach, num) %>% 
  group_by(id) %>%
  mutate(
    multy_speach = if_else(n() > 1L, T, F),
    source = "ganmarteba"
  ) %>% 
  arrange(num) %>% 
  slice(1) %>% 
  select(-num) %>% 
  ungroup()










# Exporting data from lingua.ge dictionary ----
# https://lingua.ge

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
      filter(meta %in% c("English", "Infinitive", "Preverb", "Participle")) %>% 
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
               word  = clear_div_vector[num_form_vector1 + .x],
               numb = c(rep(1, 3), rep(2, 3)),
               prsn = rep(1:3, 2),
               tense = str_remove_all(clear_div_vector[.x], "[ა-ჰ]") %>% str_squish(),
             )
      )
    
    num_form_vector1 <- c(2:3, 5:7)
    num_form_vector2 <- c(129) # , 137, 145)
    
    conjugate_imperative <- 
      map_df(num_form_vector2, 
             ~ tibble(
               word  = clear_div_vector[num_form_vector1 + .x],
               numb  = c(rep(1, 2), rep(2, 3)),
               prsn  = c(2:3, 1:3),
               tense = str_remove_all(clear_div_vector[.x], "[ა-ჰ]") %>% str_squish()
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
  # digit from link for ordering
  mutate(digit = coalesce(as.integer(str_extract(link, "[[:digit:]]")), 0L)) %>% 
  # unnest infinitive
  mutate(infinitive = map(infinitive, ~ str_split(.x, "/"))) %>%
  unnest(infinitive) %>%
  mutate(infinitive = map(infinitive, ~ tibble(infinitive = .x))) %>%
  unnest(infinitive) %>% 
  mutate_at("infinitive", str_squish) %>% 
  filter(infinitive != "")
  

# verb forms
conjugate_forms <- conjugate_verbs %>% 
  reduce(add_row) %>% 
  filter(lid %in% conjugate_words$lid)

# verb forms clearing and de-duplication
conjugate_forms_ext <- conjugate_forms %>% 
  group_by(lid, tense) %>% 
  arrange(numb, prsn) %>% 
  mutate(num0 = row_number()) %>%
  ungroup() %>% 
  # slash-words unnest start
  mutate(word = map(word, ~ str_split(.x, "/"))) %>%
  unnest(word) %>%
  mutate(word = map(word, ~ tibble(word = .x))) %>%
  unnest(word) %>% 
  # slash-words unnest end
  inner_join(verb_tense_data, by = "tense") %>% 
  inner_join(conjugate_words, by = "lid") %>%
  group_by(word) %>%
  arrange(digit, str_length(infinitive), num, num0) %>% 
  mutate(priority = row_number()) %>% 
  ungroup() %>% 
  select(lid, priority, word, numb, prsn, tense)

conjugate_words_ext <- conjugate_words %>% 
  group_by(lid) %>% 
  mutate(priority = row_number(), .after = "lid") %>% 
  ungroup()






relative_words <- ganmarteba_words_ext %>% 
  # words with relative forms
  filter(!is.na(relative)) %>% 
  # plural form flag
  mutate(plural = if_else(pos1 %in% c("noun", "adj"), 1L, 0L)) %>% 
  select(id, word, relative, plural) %>% 
  # unnesting relative forms
  mutate(relative = map(relative, ~ str_split(.x, " "))) %>%
  unnest(relative) %>%
  mutate(relative = map(relative, ~ tibble(relative = .x))) %>%
  unnest(relative) %>% 
  group_by(id, word) %>%
  mutate(plural = max(plural)) %>% 
  ungroup() %>% 
  distinct() %>% 
  arrange(id)

# ending_list <- list()
verified_ending <- 
  tribble(
    ~etype, ~ending, ~ending0, 
    1, '', '-xი',
    1, 'ის', '-ს',
    1, 'ს', '-ს',
    1, 'მა', '-მა',
    1, 'მ', '-მ',
    1, 'ო', '-ო',
    2, 'საც', '-საც',
    # 2, 'ჯერ', '-ჯერ',
    2, 'ვე', '-ვე',
    3, 'ით', '-ით',
    3, 'თ', '-ით',
    3, 'ზე', '-ზე',
    3, 'ში', '-ში',
    3, 'იდან', '-დან',
    3, 'თან', '-თან',
    3, 'სთან', '-თან',
    3, 'ამდე', '-ამდე',
    3, 'ისთვის', '-თვის',
    3, 'ისათვის', '-თვის',
    3, 'სათვის', '-თვის',
    3, 'თვის', '-თვის',
    3, 'სავით', '-ვით',
    3, 'ივით', '-ვით',
    3, 'ისკენ', '-სკენ',
    3, 'სკენ', '-სკენ',
    3, 'ქვეშ', '-ქვეშ',
    3, 'ისგან', '-სგან',
    4, 'ად', '-ად',
    4, 'დ', '-ად',
    4, 'ისას', '-სას',
    4, 'სას', '-სას'
  )

word_list_temp1 <- list()
word_list_temp2 <- list()
word_list1 <- list()
word_list2 <- list()
i <- 1L
relative_id_list <- unique(relative_words$id)

for (ii in relative_id_list) {
  
  tmpl <- relative_words %>% 
    filter(id == !!ii)
  
  id0 <- tmpl$id[1]
  plural <- tmpl$plural[1]
  wrd0 <- tmpl$word[1]
  wrd1 <- tmpl$relative
  
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
  if (plural == 1L) {
    for (j in 1:length(plural_vector)) {
      
      plroot <- str_remove(plural_vector[j], ".$")
      word_list_temp1[[i]] <- raw_ka_words %>%
        select(id, wrd) %>%
        anti_join(ganmarteba_words, by = "id") %>%
        # anti_join(ending_form_table, by = "id") %>%
        filter(str_detect(wrd, paste0("^", plroot))) %>%
        filter(wrd != !!plural) %>%
        mutate(ending = str_remove(wrd, paste0("^", plroot))) %>%
        filter(str_length(ending) < 10L) %>%
        mutate(ending = str_remove(ending, "[აც]$|(აც)$")) %>% 
        inner_join(verified_ending, by = "ending") %>%
        select(-ending)
    }
    
    word_list_temp1[[j+1]] <- raw_ka_words %>%
      select(id, wrd) %>%
      anti_join(ganmarteba_words, by = "id") %>%
      filter(wrd %in% !!plural_vector) %>% 
      mutate(etype = 0, ending0 = "x")
    
    word_list1[[i]] <- reduce(word_list_temp1, rbind) %>% mutate(id0 = !!id0, .after = 1L)
    word_list_temp1 <- list()
  }

  # Singular forms
  for (j in 1:length(root_vector)) {
    
    word_list_temp2[[j]] <- raw_ka_words %>%
      select(id, wrd) %>%
      anti_join(ganmarteba_words, by = "id") %>% 
      # anti_join(ending_form_table, by = "id") %>%
      filter(str_detect(wrd, paste0("^", root_vector[j]))) %>%
      filter(!str_detect(wrd, paste0("^", plroot))) %>%   
      filter(!wrd %in% c(!!wrd0, !!wrd1)) %>%
      mutate(ending = str_remove(wrd, paste0("^", root_vector[j]))) %>%
      filter(str_length(ending) < 10L) %>%
      mutate(ending = str_remove(ending, "[აც]$|(აც)$")) %>% 
      inner_join(verified_ending, by = "ending") %>%
      select(-ending)
  }
  
  word_list_temp2[[j+1]] <- raw_ka_words %>%
    select(id, wrd) %>%
    anti_join(ganmarteba_words, by = "id") %>% 
    filter(wrd %in% !!wrd1) %>% 
    mutate(etype = 1, ending0 = "-ის")
  
  word_list2[[i]] <- reduce(word_list_temp2, rbind) %>% mutate(id0 = !!id0, .after = 1L)
  word_list_temp2 <- list()
  i <- i + 1

}

word_buliding_plural <- reduce(word_list1, add_row) %>% add_column(numb = 2L)
word_buliding_single <- reduce(word_list2, add_row) %>% add_column(numb = 1L)
word_buliding_forms_table <- add_row(word_buliding_single, word_buliding_plural) %>% 
  distinct() %>% 
  group_by(id) %>% 
  arrange(etype, numb) %>%
  filter(row_number() == 1L) %>% 
  ungroup()
  


losty <- top_words %>% 
  anti_join(ganmarteba_words_ext, by = "id") %>% 
  anti_join(word_buliding_forms_table, by = "id") %>% 
  anti_join(conjugate_forms_ext, by = c("wrd" = "word")) %>% 
  anti_join(extra_word_forms, by = c("wrd" = "word"))

losty %>% view()

sentenses <- raw_ka_sentense %>% 
  add_column(wrd = words_from_sentense) %>% 
  unnest(wrd) %>% 
  select(-id)

sentenses %>% 
  filter(wrd == "იქცა") %>% 
  view()

losty %>% 
  left_join(sentenses, by = "wrd") %>% 
  group_by(id) %>% 
  slice(1) %>% 
  write_csv2("losty.csv")


tab1 <- ganmarteba_words_ext %>% 
  inner_join(raw_ka_words, by = c("id")) %>% 
  distinct(id, wrd, word)

tab2 <- conjugate_forms_ext %>% 
  filter(priority == 1) %>% 
  inner_join(raw_ka_words, by = c("word" = "wrd")) %>% 
  inner_join(filter(conjugate_words_ext, priority == 1), by = "lid") %>% 
  arrange(desc(frq)) %>%
  distinct(id, wrd = word, word = infinitive) %>% 
  anti_join(tab1, by = "id")

tab3 <- word_buliding_forms_table %>% 
  inner_join(raw_ka_words, by = c("id0" = "id")) %>% 
  distinct(id, wrd = wrd.x, word = wrd.y) %>% 
  anti_join(tab1, by = "id") %>% 
  anti_join(tab2, by = "id")

tab4 <- conjugate_words_ext %>% 
  filter(priority == 1) %>% 
  inner_join(raw_ka_words, by = c("infinitive" = "wrd")) %>% 
  distinct(id, wrd = infinitive, word = infinitive) %>% 
  anti_join(tab1, by = "id") %>% 
  anti_join(tab2, by = "id") %>% 
  anti_join(tab3, by = "id")


total <- sum(raw_ka_words$frq)
bigt <- rbind(tab1, tab2, tab3, tab4) %>% 
  distinct() %>% 
  inner_join(raw_ka_words, by = c("id")) %>% 
  group_by(word) %>% 
  summarise_at(vars(frq, frq_film), sum, na.rm = T) %>%
  arrange(desc(frq)) %>% 
  mutate(frq2 = cumsum(frq)) %>% 
  select(word, frq2) %>% 
  mutate(num = row_number(), .before = 1L)

rating <- rbind(tab1, tab2, tab3, tab4) %>% 
  distinct() %>% 
  inner_join(bigt, by = "word") %>% 
  select(num, wrd)

vbigt <- rbind(tab1, tab2, tab3, tab4) %>% 
  distinct() %>% 
  inner_join(select(raw_ka_words, id, frq, frq_film), by = c("id")) %>% 
  inner_join(bigt, by = "word") %>% 
  select(num, word, word, form = wrd, frq, frq2) %>% 
  arrange(num, desc(frq)) %>% 
  group_by(num) %>%
  mutate(frq0 = round(100 * frq / sum(frq)),
         frq2 = round(100 * frq2 / !!total, 1)) %>% 
  filter(frq0 > 5) %>%
  ungroup()

raw_ka_sentense <- dbGetQuery(conn, 
                              'select t2.id, t2.txt
from text_sources t0
JOIN ka_sentences t2 ON t0.sid = t2.sid
WHERE t0.stype = "film"')
  
vbigt <- vbigt %>% add_column(txt = as.character(NA))
for (i in 1:nrow(vbigt)) {
  wrd <- vbigt$form[i]
  txt <- raw_ka_sentense %>% 
    filter(str_detect(txt, paste0(" ", !!wrd, " "))) %>% 
    arrange(str_length(txt)) %>% 
    slice(1:5) %>% 
    pull(txt)
  if (length(txt) > 0) {
    vbigt[i, "txt"] <- sample(txt, 1)[1]
  }
}


i <- 10
wrd <- vbigt$form[i]
words_from_sentense <- raw_ka_sentense$txt %>% 
  str_replace_all("[[:punct:]]", " ") %>% 
  str_split("\\s+") %>% 
  map(~ .[str_detect(.x, pattern = "[ა-ჰ]")])



sentence_rating <- raw_ka_sentense %>% 
  select(id) %>% 
  add_column(wrd = words_from_sentense) %>% 
  unnest(wrd) %>%
  left_join(rating, by = "wrd") %>% 
  group_by(id) %>% 
  summarise(
    maxn = max(num, na.rm = T),
    wcnt = n(),
    lost = sum(if_else(is.na(num), 1, 0))
  )

sentence_rating %>% 
  filter(
    lost < 2, 
    maxn > 5,
    wcnt > 1
  ) %>% 
  mutate(func = wcnt - lost * 5) %>% 
  group_by(maxn) %>% 
  arrange(desc(func)) %>% 
  filter(row_number() < 5) %>% 
  ungroup() %>%
  inner_join(raw_ka_sentense, by = "id") %>% 
  inner_join(bigt, by = c("maxn" = "num")) %>% 
  arrange(maxn) %>%
  write_csv2("tops.csv")
 


write_csv2(vbigt, "top.csv")






rbind(tab1, tab2, tab3, tab4) %>% 
  distinct() %>% 
  inner_join(raw_ka_words, by = c("id")) %>% 
  group_by(word) %>% 
  summarise_at(vars(frq, frq_film), sum) %>% 
  arrange(desc(frq_film)) %>% 
  write_csv2("top.csv", )


raw_ka_words %>% 
  inner_join(word_buliding_forms_table, by = "id") %>% 

top_words %>% 
  anti_join(ganmarteba_words_ext, by = "id") %>% 
  anti_join(conjugate_forms, by = c("wrd" = "word")) %>% 
  anti_join(word_buliding_forms_table, by = "id") %>% 
  arrange(id) %>% 
  write_csv2("lost.csv")

ganmarteba_words_ext %>% 
  filter(pos1 == "pron") %>% 
  view()


raw_ka_words %>% 
  filter(str_detect(wrd, "^იმ")) %>% 
  view()
