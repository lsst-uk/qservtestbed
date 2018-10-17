/* 4 (query 013) - Find all objects within ? arcseconds of one another that have very similar colors */

SELECT DISTINCT o1.sourceID AS s1, o2.sourceID AS s2
FROM UKIDSSDR8_dxsSource as o1, 
 UKIDSSDR8_dxsSource as o2
WHERE scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) < 0.0167
 AND o1.sourceID <> o2.sourceID
 AND ABS( o1.jmhPnt - o2.jmhPnt ) < 0.5
 AND ABS( o1.hmkPnt - o2.hmkPnt ) < 0.5