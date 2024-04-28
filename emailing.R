library(blastula)
library(glue)
library(yaml)
library(DBI)
library(tidyverse)

email_secrect <- yaml::read_yaml("email.yaml") # secret config based on list() structure
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")


verified_oid <- distinct(ka_word_tidy_dict, wid, oid, pos) 
freq_oid <- raw_ka_words %>% 
  inner_join(verified_oid, by = "wid") %>% 
  group_by(oid, pos) %>% 
  summarise(ofrq = sum(frq, na.rm = T), .groups = "drop") %>% 
  group_by(pos) %>% 
  mutate(topn = row_number(desc(ofrq))) %>% 
  ungroup()

max_freq_oid <- freq_oid %>% 
  group_by(oid) %>% 
  summarise_at(vars(ofrq), max, na.rm = T)

topn_wrd_rating <- raw_ka_words %>% 
  left_join(filter(ka_word_tidy_dict, num == 1L), by = "wid") %>% 
  mutate(oid = coalesce(oid, wid)) %>% 
  left_join(max_freq_oid, by = "oid") %>% 
  mutate(topn = dense_rank(desc(coalesce(ofrq, frq)))) %>% 
  select(wid, topn)


freq_oid %>% 
  inner_join(ka_word_tidy_dict, by = c("oid", "pos")) %>% 
  filter(oid == wid, pos == "verb", num == 1L) %>% 
  arrange(desc(ofrq)) %>%
  head(500) %>% 
  view("verbs")




raw_ka_sentense <- dbGetQuery(conn, 'select id, txt from ka_sentences')
words_from_sentense <- raw_ka_sentense$txt %>% 
  str_replace_all("[[:punct:]]", " ") %>% 
  str_split("\\s+") %>% 
  map(~ .[str_detect(.x, pattern = "[ა-ჰ]")])

words_from_sentense_df <- select(raw_ka_sentense, id) %>% 
  add_column(wrd = words_from_sentense) %>% 
  unnest(wrd) %>% 
  inner_join(select(raw_ka_words, wid, wrd), by = "wrd")

sentense_hardness <- words_from_sentense_df %>% 
  inner_join(topn_wrd_rating, by = "wid") %>% 
  group_by(id) %>% 
  summarise(maxy = max(topn), cnt = n())

meaning_temples <- 
  list(
    '\U0001F1F7\U0001F1FA {text}',
    '\U0001F1EC\U0001F1E7 {text}',
    '\U0001F1EC\U0001F1EA <small>{text}</small>'
  )

my_verb_oid <- 4252
header_table <- ka_word_tidy_dict %>% 
  filter(pos == "verb", num == 1L, wid == !!my_verb_oid)

header_label <- header_table %>% 
  left_join(freq_oid, by = c("oid", "pos")) %>% 
  left_join(part_of_speach_dict, by = c("pos" = "eng")) %>% 
  mutate(heading = glue("<tt><b>\U0001F525 #{topn}</b> {geo}</tt>")) %>% 
  pull(heading)
  
header <- header_table %>% 
  glue_data('<h1><b><span style="color: #BA2649">{word}</span></b></h1>')
  
main_forms <- ka_word_tidy_dict %>% 
  filter(pos == "verb", num == 1L, oid == !!my_verb_oid) %>% 
  filter(tid %in% c("V011", "V031", "V041")) %>% 
  select(tid, word) %>% 
  arrange(tid) %>% 
  pull(word) %>% 
  paste(collapse = " \u2192 ") %>% 
  paste0("<h3>", ., "</h3>")

meaning <- header_table %>% 
  select(rus, eng, desc) %>% 
  map2(meaning_temples,
       ~ ifelse(is.na(.x), NA, glue(.y, text = .x))
  ) %>% 
  keep(~!is.na(.)) %>%
  paste0(sep = "<br>", collapse = "")


tense_emoji <- 
  tribble(
    ~eid, ~tenseji,
    "01", "\u231B",
    "02", "\U0001F570\U000FE0F",
    "03", "\U0001F570\U000FE0F\U0001F51A",
    "04", "\U0001F680\U0001F51C",
    "05", "\U0001F300",
    "06", "\U0001F449",
    "X", "\U0001F4DA"
  )

num_emoji <- tribble(
  ~pid, ~numji,
  1, "\u0031\ufe0f\u20e3\U0001F464",
  2, "\u0032\ufe0f\u20e3\U0001F464",
  3, "\u0033\ufe0f\u20e3\U0001F464",
  4, "\u0031\ufe0f\u20e3\U0001F465", 
  5, "\u0032\ufe0f\u20e3\U0001F465", 
  6, "\u0033\ufe0f\u20e3\U0001F465"
)




popular_form <- ka_word_tidy_dict %>% 
  filter(pos == "verb", num == 1L, oid == !!my_verb_oid) %>% 
  left_join(raw_ka_words,  by = "wid") %>% 
  left_join(freq_oid, by = c("pos", "oid")) %>% 
  mutate(share = frq / ofrq) %>% 
  arrange(desc(frq)) %>% 
  mutate(
    eid = case_when(
      !str_detect(tid, "V") ~ "X",
      T ~ str_sub(tid, 2, 3)
    ),
    pid = case_when(
      !str_detect(tid, "V") ~ as.integer(NA), 
      T ~ as.integer(str_sub(tid, 4, 4))
    )) %>% 
  left_join(tense_emoji, by = "eid") %>% 
  left_join(num_emoji, by = "pid") %>%
  mutate(label = paste(coalesce(tenseji, "\U0001F914"), if_else(is.na(numji), "", paste("\u00D7", numji)))) %>% 
  select(tid, label, word, share) %>% 
  mutate(share = if_else(share < 0.01, "<1%", scales::percent_format(accuracy = 1)(share))) %>% 
  filter(row_number() <= 5L) %>% 
  glue_data("\u2022 {share} {word} <small>{label}</small>") %>% 
  paste0(collapse = "<br>")

part2 <- paste0("<h2>სიხშირე</h2>", popular_form)


hardness_emoji <- 
  tribble(
    ~eid, ~emoji,
    1, "<h3>\U0001F60A მარტივი</h3>", # "😊" (Easy)
    2, "<h3>\U0001F610 ზომიერი</h3>", # "😐" (Moderate)
    3, "<h3>\U0001F615 რთული</h3>",  # "😕" (Challenging)
    4, "<h3>\U0001F630 უფრო რთული</h3>", # "😰" (Difficult)
    5, "<h3>\U0001F62B ძალიან რთული</h3>", # "😫" (Very Difficult)
  )

examples_df <- ka_word_tidy_dict %>% 
  filter(pos == "verb", num == 1L, oid == !!my_verb_oid) %>% 
  inner_join(words_from_sentense_df, by = "wid") %>%
  inner_join(sentense_hardness, by = "id") %>% 
  filter(cnt > 3, maxy < 1000) %>% 
  # left_join(hardness_emoji, by = "eid") %>% 
  distinct(id, maxy, cnt, wid, word) %>%
  group_by(wid) %>%
  arrange(maxy) %>%
  filter(row_number() <= 2L) %>%
  ungroup() %>% 
  arrange(maxy, cnt, wid) %>%
  inner_join(raw_ka_sentense, by = "id") %>% 
  mutate(txt = str_squish(str_remove(txt, "^[[:punct:]]"))) %>% 
  group_by(txt) %>% 
  sample_n(1L) %>% 
  ungroup() %>% 
  mutate(txt = paste("\u2022", str_replace_all(txt, word, glue('<span style="color: #BA2649">{word}</span>')))) %>% 
  mutate(eid = cut(maxy, breaks = c(0, 250, 500, 1000, 5000, Inf), labels = FALSE)) %>%
  group_by(eid) %>% 
  filter(row_number() <= 5L) %>% 
  ungroup()

examples <- examples_df %>% 
  nest(data = -eid) %>% 
  arrange(eid) %>% 
  inner_join(hardness_emoji, by = "eid") %>% 
  mutate(col = map_chr(data, ~ paste0(glue_data(., "{txt}"), collapse = "<br>"))) %>% 
  glue_data("{emoji} <p>{col}</p>") %>% 
  paste0(collapse = "")

part3 <- paste0("<h2>მაგალითები</h2>", examples)

# extra details
html_footer <- paste(
  "Kartuli v1.0", "<br>",
  "Created by Artem R.", "<br>",
  "\U0001F419",
  '[**GitHub**](https://github.com/artemRa/kartuli)'
)

composed_email <- 
  compose_email(
    header = md(header_label),
    body = list(md(header), md(main_forms), md(meaning), md(part2), md(part3)),
    # body = list(md(email_words_list), md(html_labels), md(html_checking), md(html_praise)),
    footer = md(html_footer)
  )


composed_email

Sys.setenv(SMTP_PASSWORD = email_secrect$password) # pass_envvar
composed_email %>% 
  smtp_send(
    from = c("kartuli robot" = email_secrect$username),
    to = email_secrect$to,
    subject = "new verb",
    credentials = creds_envvar(
      user = email_secrect$username,
      provider = "gmail"
    )
  )
