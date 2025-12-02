SELECT e.seasonnumber, e.episodenumber, b.primarytitle, r.averagerating
FROM title_episode e
JOIN title_basics b ON e.tconst = b.tconst
JOIN title_ratings r ON e.tconst = r.tconst
WHERE e.parenttconst = 'tt0903747'
ORDER BY seasonnumber, episodenumber;
