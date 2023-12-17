-- !preview conn=DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")

SELECT sid, count(*) 
FROM ka_sentences
GROUP BY sid