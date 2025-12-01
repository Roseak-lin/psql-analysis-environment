EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, TIMING, format TEXT) 
select * from title_basics where titletype = 'short' and originaltitle like '%malice%';