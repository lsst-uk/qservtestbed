-- 6 - Find all galaxies in dense regions (query 025)

SELECT DISTINCT s1.sourceID, s1.ra, s1.dec
FROM   dxsSource s1, dxsSource s2
WHERE (
        SELECT COUNT(s3.sourceID)
        FROM   dxsSource s3
        WHERE  s1.sourceID <> s3.sourceID
          AND  ABS(s1.ra   - s3.ra  ) < 0.1/COS(RADIANS(s3.dec))
          AND  ABS(s1.dec - s3.dec) < 0.1
       ) > 5000
AND s1.pGalaxy > 0.99
AND s2.pGalaxy > 0.99

/*SELECT DISTINCT s1.objectId, s1.ra, s1.dec, s2.iauId
FROM   Object s1, Object s2
WHERE  ABS(s2.ra   - s1.ra  ) < s2.raRange/(2*COS(RADIANS(s1.dec)))
   AND ABS(s2.dec - s1.dec) < s2.decRange/2 
   AND (
        SELECT COUNT(s3.objectId)
        FROM   Object s3
        WHERE  s1.objectId <> s3.objectId
          AND  ABS(s1.ra   - s3.ra  ) < 0.1/COS(RADIANS(s3.dec))
          AND  ABS(s1.dec - s3.dec) < 0.1
       ) > 10000;*/

