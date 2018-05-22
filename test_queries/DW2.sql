/* 3. (query 047) - Select all variable objects within certain distance (e.g. 0.1 arcmin) of all known galaxies*/

SELECT v.sourceID, v.ra, v.dec, d.sourceID, d.ra, d.dec, scisql_angSep(d.ra, d.dec, v.ra, v.dec)
FROM dxsVariability AS v, dxsSource AS d
WHERE v.sourceID <> d.sourceID
 AND d.pGalaxy > 0.99
 AND v.jprobVar > 0.9
 AND v.ra > 0.0
 AND scisql_angSep(d.ra, d.dec, v.ra, v.dec) < 0.1
