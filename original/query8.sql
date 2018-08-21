/* 8 - Search for merging galaxy pairs, as per the prescription in Allam et al. 2004 (query 034): */

SELECT COUNT(*)
FROM  Galaxy    g1,
JOIN  Neighbors n  USING (objectId)
JOIN  Galaxy    g2 ON (g2.objectId = N.NeighborObjID)
WHERE g1.objectId < g2.objectId
   AND N.NeighborType = 3
   AND g1.petrorad_u > 0 AND g2.petrorad_u > 0
   AND g1.petrorad_g > 0 AND g2.petrorad_g > 0
   AND g1.petrorad_r > 0 AND g2.petrorad_r > 0
   AND g1.petrorad_i > 0 AND g2.petrorad_i > 0
   AND g1.petrorad_z > 0 AND g2.petrorad_z > 0
   AND g1.petroradErr_g > 0 AND g2.petroradErr_g > 0
   AND g1.petroMag_g BETWEEN 16 AND 21
   AND g2.petroMag_g BETWEEN 16 AND 21
   AND g1.uMag > -9999
   AND g1.gMag > -9999
   AND g1.rMag > -9999
   AND g1.iMag > -9999
   AND g1.zMag > -9999
   AND g1.yMag > -9999
   AND g2.uMag > -9999
   AND g2.gMag > -9999
   AND g2.rMag > -9999
   AND g2.iMag > -9999
   AND g2.zMag > -9999
   AND g2.yMag > -9999
   AND abs(g1.gMag - g2.gMag) > 3
   AND (g1.petroR50_r BETWEEN 0.25*g2.petroR50_r AND 4.0*g2.petroR50_r)
   AND (g2.petroR50_r BETWEEN 0.25*g1.petroR50_r AND 4.0*g1.petroR50_r)
   AND (n.distance <= (g1.petroR50_r + g2.petroR50_r))
