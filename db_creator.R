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
