/* 8 (query 034) - Search for merging galaxy pairs, AS per the prescription in Allam et al. 2004 */

SELECT g1.objID AS Gal1_ID, g2.objID AS Gal2_ID
FROM BestDR7_PhotoObjAll.PhotoObjAll g1, BestDR7_PhotoObjAll.PhotoObjAll g2, BestDr7.Photoz n	   
WHERE 
   g1.type = 3 AND g2.type = 3
   AND g1.objID = n.objID
   AND g2.objID = n.nnObjID
   AND g1.objId < g2.ObjID
   AND g1.petrorad_u > 0 AND g2.petrorad_u > 0
   AND g1.petrorad_g > 0 AND g2.petrorad_g > 0
   AND g1.petrorad_r > 0 AND g2.petrorad_r > 0
   AND g1.petrorad_i > 0 AND g2.petrorad_i > 0
   AND g1.petrorad_z > 0 AND g2.petrorad_z > 0
   AND g1.petroradErr_g > 0 AND g2.petroradErr_g > 0
   AND g1.petroMag_g BETWEEN 16 AND 21
   AND g2.petroMag_g BETWEEN 16 AND 21
   AND g1.modelmag_u > -9999
   AND g1.modelmag_g > -9999
   AND g1.modelmag_r > -9999
   AND g1.modelmag_i > -9999
   AND g1.modelmag_z > -9999
   AND g2.modelmag_u > -9999
   AND g2.modelmag_g > -9999
   AND g2.modelmag_r > -9999
   AND g2.modelmag_i > -9999
   AND g2.modelmag_z > -9999
   AND abs(g1.modelmag_g - g2.modelmag_g) > 3
   AND (g1.petroR50_r BETWEEN 0.25*g2.petroR50_r AND 4.0*g2.petroR50_r)
   AND (g2.petroR50_r BETWEEN 0.25*g1.petroR50_r AND 4.0*g1.petroR50_r)
   AND (scisql_angSep(g1.ra, g1.decl, g2.ra, g2.decl)/3600.0 <= (g1.petroR50_r + g2.petroR50_r))
   AND scisql_angSep(g1.ra, g1.decl, g2.ra, g2.decl) < 0.1