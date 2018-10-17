-- WORKING but missing specifics - not fully applicable to UKIDSS data

/* 8 (query 034) - Search for merging galaxy pairs, as per the prescription in Allam et al. 2004 */

SELECT s1.sourceID, s2.sourceID, n.distanceMins

FROM dxsSource AS s1, dxsSourceNeighbours AS n, dxsSource AS s2 

WHERE (s1.sourceID = n.masterObjID) 
AND (s2.sourceID = n.slaveObjID)
AND s1.sourceID < s2.sourceID
AND s1.mergedClass = 1 -- same as pGalaxy>90?
AND s2.mergedClass = 1
AND (s1.jGausig BETWEEN 0.25*s2.jGausig AND 4.0*s2.jGausig)
AND (s2.jGausig BETWEEN 0.25*s1.jGausig AND 4.0*s1.jGausig)
AND (n.distance <= (s1.jGausig + s2.jGausig))

ORDER BY n.distanceMins


/*
AND N.NeighborType = 3
   AND s1.petrorad_u > 0 AND s2.petrorad_u > 0
   AND s1.petrorad_g > 0 AND s2.petrorad_g > 0
   AND s1.petrorad_r > 0 AND s2.petrorad_r > 0
   AND s1.petrorad_i > 0 AND s2.petrorad_i > 0
   AND s1.petrorad_z > 0 AND s2.petrorad_z > 0
   AND s1.petroradErr_g > 0 AND s2.petroradErr_g > 0
   AND s1.petroMag_g BETWEEN 16 AND 21
   AND s2.petroMag_g BETWEEN 16 AND 21
   AND s1.uMag > -9999
   AND s1.gMag > -9999
   AND s1.rMag > -9999
   AND s1.iMag > -9999
   AND s1.zMag > -9999
   AND s1.yMag > -9999
   AND s2.uMag > -9999
   AND s2.gMag > -9999
   AND s2.rMag > -9999
   AND s2.iMag > -9999
   AND s2.zMag > -9999
   AND s2.yMag > -9999
   AND abs(s1.gMag - s2.gMag) > 3
   AND (s1.petroR50_r BETWEEN 0.25*s2.petroR50_r AND 4.0*s2.petroR50_r)
   AND (s2.petroR50_r BETWEEN 0.25*s1.petroR50_r AND 4.0*s1.petroR50_r)
   AND (n.distance <= (s1.petroR50_r + s2.petroR50_r))
   */