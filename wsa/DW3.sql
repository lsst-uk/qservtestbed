/* 3. (query 047) - Select all variable objects within certain distance (e.g. 0.1 arcmin) of all known galaxies*/

SELECT v.sourceID as vID, v.ra as vRA, v.dec as vDec, g.sourceID as dID, g.ra as dRA, g.dec as dDec, dbo.fGreatCircleDist(g.ra, g.dec, v.ra, v.dec) as angsep

FROM dxsVariability AS v, dxsSource AS g

WHERE v.sourceID <> g.sourceID
 AND g.pGalaxy > 0.99
 AND v.jprobVar > 0.9
 AND v.ra > 0.0
 AND dbo.fGreatCircleDist(g.ra, g.dec, v.ra, v.dec) < 0.1
