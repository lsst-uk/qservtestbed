/* 3. (query 047) - Select all variable objects within certain distance (e.g. 0.1 arcmin) of all known galaxies*/

SELECT v.sourceID as vID, v.ra as vRA, v.dec as vDec, d.sourceID as dID, d.ra as dRA, d.dec as dDec, scisql_angSep(d.ra, d.dec, v.ra, v.dec) as angsep
FROM dxsVariability AS v, dxsSource AS d
WHERE v.sourceID <> d.sourceID
 AND d.pGalaxy > 0.99
 AND v.jprobVar > 0.9
 AND v.ra > 0.0
 AND scisql_angSep(d.ra, d.dec, v.ra, v.dec) < 0.00167
