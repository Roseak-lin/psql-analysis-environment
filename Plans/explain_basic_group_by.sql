EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, TIMING, format TEXT)
SELECT 
    COUNT(tconst), 
    titletype 
FROM 
    title_basics 
WHERE 
    startyear = (random() * 34 + 1990)::integer
GROUP BY 
    titletype;