/* 6 - Find all galaxies in dense regions (query 025)  */

SELECT DISTINCT o1.objectId, o1.ra, o1.decl, o2.iauId
FROM   Object o1, Object o2
WHERE  ABS(o2.ra   - o1.ra  ) < o2.raRange/(2*COS(RADIANS(o1.decl)))
   AND ABS(o2.decl - o1.decl) < o2.declRange/2 
   AND (
        SELECT COUNT(o3.objectId)
        FROM   Object o3
        WHERE  o1.objectId <> o3.objectId
          AND  ABS(o1.ra   - o3.ra  ) < 0.1/COS(RADIANS(o3.decl))
          AND  ABS(o1.decl - o3.decl) < 0.1
       ) > 10000;
