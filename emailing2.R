library(blastula)
library(glue)
library(yaml)
library(DBI)
library(tidyverse)

email_secrect <- yaml::read_yaml("email.yaml") # secret config based on list() structure
config <- yaml::read_yaml("secret.yaml")
verb_tense_data <- gsheet::gsheet2tbl(config$glink1)
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
rarity_cutoff <- 5L
raw_ka_words <- dbGetQuery(conn, "SELECT * FROM ka_words_sample")
ka_word_tidy_dict <- dbGetQuery(conn, "SELECT * FROM ka_word_tidy_dict")
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
  # mutate(id = row_number(), .before = 1L) %>% 
  # filter(!is.na(eng)) %>% 
  head(500) %>% 
  view("verbs")


# Collecting examples of word usage to understand context ----
## Collecting all available examples ----
my_verb_oid <- 42551
raw_ka_sentense <- dbGetQuery(conn, 'select id, txt from ka_sentences')
words_from_sentense <- raw_ka_sentense$txt %>% 
  str_replace_all("[[:punct:]]", " ") %>% 
  str_split("\\s+") %>% 
  map(~ .[str_detect(.x, pattern = "[ა-ჰ]")])

## Labeling the complexity of sentences ----
words_from_sentense_df <- select(raw_ka_sentense, id) %>% 
  add_column(wrd = words_from_sentense) %>% 
  unnest(wrd) %>% 
  inner_join(select(raw_ka_words, wid, wrd), by = "wrd")

sentense_hardness <- words_from_sentense_df %>% 
  inner_join(topn_wrd_rating, by = "wid") %>% 
  anti_join(filter(ka_word_tidy_dict, oid == !!my_verb_oid), by = "wid") %>% 
  group_by(id) %>% 
  summarise(maxy = max(topn), cnt = n())


## Finding contextually related words ----
# Collecting words present in the same sentence as the target word
sputnik_words_frq_pre <- ka_word_tidy_dict %>% 
  filter(pos == "verb", oid == !!my_verb_oid) %>% 
  distinct(wid, word) %>% 
  inner_join(words_from_sentense_df, by = "wid") %>%
  distinct(id, word) %>% 
  inner_join(words_from_sentense_df, by = "id") %>% 
  filter(word != wrd)

# Searching for frequency anomalies
sputnik_words_frq <- sputnik_words_frq_pre %>% 
  count(word, wid) %>%
  filter(n / sum(n) > 0.001, n > 5)

top_word_connection <- sputnik_words_frq %>% 
  inner_join(raw_ka_words, by = "wid") %>%
  group_by(word) %>% 
  mutate_at(vars(n, frq), ~ .x / sum(.x)) %>% 
  mutate(dev = n / frq) %>%
  filter(src > 1) %>%
  arrange(desc(dev)) %>%
  filter(row_number() <= 30L, dev > 1.3) %>%
  select(word, wid, wrd, dev)


context_add_needed <- sputnik_words_frq_pre %>% select(wid, id) %>%  
  inner_join(top_word_connection, by = "wid") %>% 
  group_by(id, word) %>% 
  summarise(score = max(dev), .groups = "drop")
  
replacer <- function(data, word_vector) {
  for (i in 1:length(word_vector)) {
    wrdy <- word_vector[i]
    data <- data %>%
      mutate(
        txt = str_replace_all(txt, paste0("\\b", !!wrdy, "(\\b|[[:punct:]])"), glue('<u>{wrdy}</u>'))
      )
  }
  data
}

tense_emoji <- 
  tribble(
    ~eid, ~tenseji,
    "01", "\u26A1",
    "02", "\U0001F570\U000FE0F",
    "03", "\U0001F570\U000FE0F\U0001F51A",
    "04", "\U0001F680\U0001F51C",
    "05", "\U0001F300",
    "06", "\U0001F449",
    "X", "\u0030\uFE0F\u20E3"
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

tense_num_emoji <- crossing(tense_emoji, num_emoji) %>% 
  filter(eid != "X") %>% 
  mutate(
    tid = paste0("V", eid, pid),
    label = paste0(tenseji, numji)
  ) %>% 
  select(tid, label)


examples_df <- ka_word_tidy_dict %>% 
  filter(pos == "verb", oid == !!my_verb_oid) %>%
  mutate(vsimple = !str_detect(tid, "V") | as.numeric(str_sub(tid, 2, 3)) <= 6L) %>% 
  filter(vsimple == 1) %>% 
  inner_join(words_from_sentense_df, by = "wid") %>%
  inner_join(sentense_hardness, by = "id") %>%
  filter(cnt > 3, maxy < 1000) %>%
  distinct(id, maxy, cnt, tid, wid, word)

top_sentenses_lv1 <- examples_df %>%
  left_join(context_add_needed, by = c("id", "word")) %>%
  replace_na(list(score = 0)) %>%
  mutate(dr = dense_rank(desc(score))) %>%
  group_by(dr) %>% 
  arrange(maxy, cnt, wid) %>% 
  slice(1L) %>% 
  ungroup() %>% 
  filter(dr <= 10) %>% 
  select(id, maxy, cnt, tid, wid, word)

top_sentenses_lv2 <- examples_df %>% 
  filter(!word %in% top_sentenses_lv1$word) %>% 
  anti_join(top_sentenses_lv1, by = "id") %>% 
  group_by(wid) %>%
  arrange(maxy, cnt, wid) %>% 
  slice(1L) %>% 
  ungroup()

top_sputnik_words_vector <- top_word_connection %>% 
  filter(row_number(desc(dev)) <= 10) %>% 
  pull(wrd)

top_sentenses_full <- top_sentenses_lv2 %>% 
  add_row(top_sentenses_lv1) %>% 
  inner_join(raw_ka_sentense, by = "id") %>% 
  mutate(txt = str_squish(str_remove(txt, "^[^ა-ჰ0-9]+"))) %>%
  mutate(tech_txt = str_squish(str_remove_all(txt, "[[:punct:]]"))) %>% 
  group_by(tech_txt) %>% 
  sample_n(1L) %>%
  ungroup() %>%
  mutate(eid = cut(maxy, breaks = c(0, 250, 500, 1000, 5000, Inf), labels = FALSE)) %>%
  group_by(eid) %>%
  filter(row_number() <= 5L) %>%
  mutate(txt = paste("\u2022", str_replace_all(txt, word, glue('<span style="color: #BA2649">{word}</span>')))) %>%
  replacer(top_sputnik_words_vector) %>%
  left_join(tense_num_emoji, by = "tid") %>%
  mutate(txt = paste(txt, "<br>&nbsp;<b>/<small>", label, "<br></small></b>")) %>% 
  ungroup()

hardness_emoji <- 
  tribble(
    ~eid, ~emoji,
    1, "<h3>\U0001F60A მარტივი</h3>", # "😊" (Easy)
    2, "<h3>\U0001F610 ზომიერი</h3>", # "😐" (Moderate)
    3, "<h3>\U0001F615 რთული</h3>",  # "😕" (Challenging)
    4, "<h3>\U0001F630 უფრო რთული</h3>", # "😰" (Difficult)
    5, "<h3>\U0001F62B ძალიან რთული</h3>", # "😫" (Very Difficult)
  )

hardness_emoji <- 
  tribble(
    ~eid, ~emoji,
    1, "<h3>1. მარტივი</h3>", # "😊" (Easy)
    2, "<h3>2. ზომიერი</h3>", # "😐" (Moderate)
    3, "<h3>3. რთული</h3>",  # "😕" (Challenging)
    4, "<h3>\U0001F630 უფრო რთული</h3>", # "😰" (Difficult)
    5, "<h3>\U0001F62B ძალიან რთული</h3>", # "😫" (Very Difficult)
  )

examples <- top_sentenses_full %>% 
  nest(data = -eid) %>% 
  arrange(eid) %>% 
  inner_join(hardness_emoji, by = "eid") %>% 
  mutate(col = map_chr(data, ~ paste0(glue_data(., "{txt}"), collapse = "<br>"))) %>% 
  glue_data("{emoji} <p>{col}</p>") %>% 
  paste0(collapse = "")

sputnik_top_words <- top_word_connection %>% 
  head(10) %>% 
  glue_data("{wrd}") %>% 
  paste0(collapse = ", ") %>% 
  paste0("\U1F9F5", .)

part3 <- paste0("<h2>მაგალითები</h2>", sputnik_top_words, examples)

composed_email <- 
  compose_email(
    body = md(part3)
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
