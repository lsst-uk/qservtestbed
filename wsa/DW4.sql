/* 4 (query 013) - Find all objects within 0.1 arcseconds of one another that have very similar colors */

SELECT DISTINCT s1.sourceID AS ID_1, s2.sourceID AS ID_2

FROM dxsSource as s1, dxsSource as s2

WHERE dbo.fGreatCircleDist(s1.ra, s1.dec, s2.ra, s2.dec) < 0.1
 AND s1.sourceID <> s2.sourceID
 AND ABS( s1.jmhPnt - s2.jmhPnt ) < 0.5
 AND ABS( s1.hmkPnt - s2.hmkPnt ) < 0.5
 AND ABS( s1.jmkPnt - s2.jmkPnt ) < 0.5
