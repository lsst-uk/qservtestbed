-- WORKING

/* 3. (query 047) - Select all variable objects within certain distance (e.g. 0.1 arcmin) of all known galaxies*/

SELECT v.sourceID as vID, v.ra as vRA, v.dec as vDec, s.sourceID as dID, s.ra as dRA, s.dec as dDec, dbo.fGreatCircleDist(s.ra, s.dec, v.ra, v.dec) as angsep

FROM dxsVariability AS v, dxsSource AS s

WHERE v.sourceID <> s.sourceID
 AND s.pGalaxy > 0.99
 AND v.jprobVar > 0.9
 AND v.ra > 0.0
 AND dbo.fGreatCircleDist(s.ra, s.dec, v.ra, v.dec) < 0.1
