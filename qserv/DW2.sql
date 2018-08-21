/* 3. (query 047) - Select all variable objects within certain distance (e.g. 0.1 arcmin) of all known galaxies*/

SELECT v.sourceID as vID, v.ra as vRA, v.decl as vDec, d.sourceID as dID, d.ra as dRA, d.decl as dDec, scisql_angSep(d.ra, d.decl, v.ra, v.decl) as angsep
FROM UKIDSS_Qservp.dxsVariability AS v, UKIDSS_Qservp_dxsSource.dxsSource AS d
WHERE v.sourceID <> d.sourceID
 AND d.pGalaxy > 0.99
 AND v.jprobVar > 0.9
 AND v.ra > 0.0
 AND scisql_angSep(d.ra, d.decl, v.ra, v.decl) < 0.00167
