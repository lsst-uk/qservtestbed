/* 3 - Select all variable objects within certain distance of all known galaxies (query 047): */

SELECT v.objectId
FROM   Object g, Object v
WHERE  g.extendedParam > 0.8 -- is galaxy
  AND  v.variability > 0.8   -- is variable
  AND  spDist(g.ra, g.decl, v.ra, v.decl) < :distance
