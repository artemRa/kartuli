library(DBI)

conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")
# DBI::dbSendQuery(conn, "DROP TABLE ka_sentences")
# DBI::dbSendQuery(conn, "DROP TABLE ka_words")
# DBI::dbSendQuery(conn, "DROP TABLE movie_sources")

# georgian sentences
DBI::dbSendQuery(
  conn,
  "
    CREATE TABLE ka_sentences (
      id    INTEGER,
      stype TEXT,
      sid   INTEGER,
      txt   TEXT,
      PRIMARY KEY (id)
    )
  "
)

# georgian words
DBI::dbSendQuery(
  conn,
  "
    CREATE TABLE ka_words (
      id    INTEGER,
      stype TEXT,
      sid   INTEGER,
      wrd   TEXT,
      frq   INTEGER,
      PRIMARY KEY (id)
    )
  "
)

# movies sources
DBI::dbSendQuery(
  conn,
  "
    CREATE TABLE movie_sources (
      sid   INTEGER,
      name  TEXT,
      file  TEXT,
      PRIMARY KEY (sid)
    )
  "
)

# decided make it more universal
DBI::dbSendQuery(
  conn,
  "ALTER TABLE movie_sources RENAME TO text_sources"
)

# decided to change table a bit
DBI::dbSendQuery(
  conn,
  "ALTER TABLE text_sources
  ADD COLUMN stype TEXT AFTER sid"
)

DBI::dbSendQuery(
  conn,
  "UPDATE text_sources
  SET stype = 'wiki'
  WHERE file is null"
)

DBI::dbSendQuery(
  conn,
  "UPDATE text_sources
  SET stype = 'book'
  WHERE file like '%pdf'"
)

DBI::dbSendQuery(
  conn,
  "UPDATE text_sources
  SET stype = 'film'
  WHERE stype is null"
)

DBI::dbSendQuery(
  conn,
  "ALTER TABLE ka_sentences
  DROP COLUMN stype"
)

DBI::dbSendQuery(
  conn,
  "ALTER TABLE ka_words
  DROP COLUMN stype"
)

# DBI::dbSendQuery(
#   conn,
#   "DELETE FROM ka_sentences
#   WHERE sid in (SELECT sid FROM text_sources WHERE stype = 'wiki')
#   "
# )
# 
# DBI::dbSendQuery(
#   conn,
#   "DELETE FROM text_sources
#   WHERE stype = 'wiki'
#   "
# )


DBI::dbSendQuery(
  conn,
  "
    CREATE TABLE ka_pseudo_words (
      id    INTEGER,
      wrd   TEXT,
      wrd2  TEXT,
      type  TEXT,
      PRIMARY KEY (id)
    )
  "
)

# Real raw word dict
DBI::dbSendQuery(
  conn,
  "
    CREATE TABLE ka_words_sample (
      wid INTEGER,
      wrd TEXT,
      frq INTEGER,
      src INTEGER,
      PRIMARY KEY (wid)
    )
  "
)

dbExecute(conn, {
  "
  INSERT INTO ka_words_sample
  SELECT row_number() over (order by frq desc, src desc) as wid
  , t.*
  FROM
  (
    SELECT wrd
    , sum(frq) as frq
    , count(distinct sid) as src
    FROM ka_words 
    GROUP BY wrd
  ) t
  "
})

DBI::dbSendQuery(
  conn,
  "
    CREATE TABLE ka_word_tidy_dict (
      wid INTEGER,
      oid INTEGER,
      word TEXT,
      pos TEXT,
      multypos BOOLEAN,
      source TEXT,
      desc TEXT,
      type TEXT,
      eng TEXT,
      rus TEXT,
      PRIMARY KEY (wid)
    )
  "
)

# Raw word dictionary
dbExecute(conn, {
  "
    CREATE TABLE ka_raw_word_dict AS
    SELECT row_number() over (order by frq desc) as id
    , t.*
    FROM 
    (
      SELECT coalesce(p.wrd, t2.wrd) as wrd
      , sum(t2.frq) as frq
      , count(distinct t0.sid) as srcs
      , sum(case when t0.stype = 'book' then t2.frq end) as frq_book
      , sum(case when t0.stype = 'film' then t2.frq end) as frq_film
      , sum(case when t0.stype = 'wiki' then t2.frq end) as frq_wiki
      , count(distinct case when t0.stype = 'book' then t0.sid end) as book_srcs
      , count(distinct case when t0.stype = 'film' then t0.sid end) as film_srcs
      , count(distinct case when t0.stype = 'wiki' then t0.sid end) as wiki_srcs
      FROM ka_words t2 
      JOIN text_sources t0 ON t0.sid = t2.sid
      LEFT JOIN ka_pseudo_words p on t2.wrd = p.wrd2
      GROUP BY coalesce(p.wrd, t2.wrd)
    ) t
    WHERE book_srcs + film_srcs > 5 or wiki_srcs > 100
    ORDER BY frq DESC
    "})
dbExecute(conn, {
  "
    CREATE TABLE ka_raw_word_meta AS
    SELECT 
      case when book_srcs + film_srcs > 5 or wiki_srcs > 100 
        then 'normal' else 'exotic' 
      end as wtype
    , sum(frq) as frq
    , sum(frq_book) as frq_book
    , sum(frq_film) as frq_film
    , sum(frq_wiki) as frq_wiki
    FROM 
    (
      SELECT coalesce(p.wrd, t2.wrd) as wrd
      , sum(t2.frq) as frq
      , count(distinct t0.sid) as srcs
      , sum(case when t0.stype = 'book' then t2.frq end) as frq_book
      , sum(case when t0.stype = 'film' then t2.frq end) as frq_film
      , sum(case when t0.stype = 'wiki' then t2.frq end) as frq_wiki
      , count(distinct case when t0.stype = 'book' then t0.sid end) as book_srcs
      , count(distinct case when t0.stype = 'film' then t0.sid end) as film_srcs
      , count(distinct case when t0.stype = 'wiki' then t0.sid end) as wiki_srcs
      FROM ka_words t2 
      JOIN text_sources t0 ON t0.sid = t2.sid
      LEFT JOIN ka_pseudo_words p on t2.wrd = p.wrd2
      GROUP BY coalesce(p.wrd, t2.wrd)
    ) t
    GROUP BY
      case when book_srcs + film_srcs > 5 or wiki_srcs > 100 
        then 'normal' else 'exotic' 
      end
    "})