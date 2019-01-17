/* 3. (query 047) - Select all variable objects within certain distance (e.g. 0.1 arcmin) of all known galaxies*/

SELECT v.sourceID as vID, v.ra as vRA, v.decl as vDec, g.sourceID as dID, g.ra as dRA, g.decl as dDec, scisql_angSep(g.ra, g.decl, v.ra, v.decl) as angsep

FROM UKIDSSDR8_dxsSource.dxsSource AS g, UKIDSSDR8.dxsVariability AS v

WHERE v.sourceID <> g.sourceID
 AND g.pGalaxy > 0.99
 AND v.jprobVar > 0.9
 AND v.ra > 0.0
 AND scisql_angSep(g.ra, g.decl, v.ra, v.decl) < 0.00167
