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
