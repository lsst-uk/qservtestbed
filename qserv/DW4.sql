/* 4 (query 013) - Find all objects within ? arcseconds of one another that have very similar colors */

SELECT DISTINCT s1.sourceID AS ID_1, s2.sourceID AS ID_2

FROM UKIDSSDR8_dxsSource.dxsSource as s1, 
 UKIDSSDR8_dxsSource.dxsSource as s2

WHERE scisql_angSep(s1.ra, s1.decl, s2.ra, s2.decl) < 0.0167
 AND s1.sourceID <> s2.sourceID
 AND ABS( s1.jmhPnt - s2.jmhPnt ) < 0.5
 AND ABS( s1.hmkPnt - s2.hmkPnt ) < 0.5
