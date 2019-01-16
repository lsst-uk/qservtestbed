-- 6 - Find all galaxies in dense regions (query 025)

SELECT DISTINCT s1.sourceID, s1.ra, s1.dec
FROM   dxsSource_test1.dxsSource s1, dxsSource_test1.dxsSource s2
WHERE (
        SELECT COUNT(s3.sourceID)
        FROM   dxsSource_test1.dxsSource s3
        WHERE  s1.sourceID <> s3.sourceID
          AND  ABS(s1.ra   - s3.ra  ) < 0.1/COS(RADIANS(s3.dec))
          AND  ABS(s1.dec - s3.dec) < 0.1
       ) > 5000
AND s1.pGalaxy > 0.99
AND s2.pGalaxy > 0.99