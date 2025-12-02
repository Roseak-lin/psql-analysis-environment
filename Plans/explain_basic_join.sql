EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, TIMING, format TEXT)
select tb.primarytitle, tb.startyear, tr.averagerating from title_ratings as tr
join title_basics as tb on
tb.tconst = tr.tconst
where tr.averagerating > 8.0;