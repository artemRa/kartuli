-- !preview conn=DBI::dbConnect(RSQLite::SQLite(), dbname = "kartuli.db")

/*
SELECT sid, count(*) 
FROM ka_sentences
GROUP BY sid

select count(*) 
from ka_sentences
--where sid = 23
--order by random()

select count(*), 
from ka_words
*/ 

select count(*), count(distinct wrd)
from ka_words 