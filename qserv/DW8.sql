/* 8 (query 034) - Search for merging galaxy pairs, as per the prescription in Allam et al. 2004 */

select g1.objID as Gal1_ID, g2.objID as Gal2_ID
from BestDR7_PhotoObjAll.PhotoObjAll g1, BestDR7_PhotoObjAll.PhotoObjAll g2, BestDr7.Photoz n	   
where 
   g1.type = 3 and g2.type = 3
   and g1.objID = n.objID
   and g2.objID = n.nnObjID
   and g1.objId < g2.ObjID
   and g1.petrorad_u > 0 and g2.petrorad_u > 0
   and g1.petrorad_g > 0 and g2.petrorad_g > 0
   and g1.petrorad_r > 0 and g2.petrorad_r > 0
   and g1.petrorad_i > 0 and g2.petrorad_i > 0
   and g1.petrorad_z > 0 and g2.petrorad_z > 0
   and g1.petroradErr_g > 0 and g2.petroradErr_g > 0
   and g1.petroMag_g BETWEEN 16 and 21
   and g2.petroMag_g BETWEEN 16 and 21
   and g1.modelmag_u > -9999
   and g1.modelmag_g > -9999
   and g1.modelmag_r > -9999
   and g1.modelmag_i > -9999
   and g1.modelmag_z > -9999
   and g2.modelmag_u > -9999
   and g2.modelmag_g > -9999
   and g2.modelmag_r > -9999
   and g2.modelmag_i > -9999
   and g2.modelmag_z > -9999
   and abs(g1.modelmag_g - g2.modelmag_g) > 3
   and (g1.petroR50_r BETWEEN 0.25*g2.petroR50_r AND 4.0*g2.petroR50_r)
   and (g2.petroR50_r BETWEEN 0.25*g1.petroR50_r AND 4.0*g1.petroR50_r)
   and (scisql_angSep(g1.ra, g1.dec, g2.ra, g2.dec)/3600.0 <= (g1.petroR50_r + g2.petroR50_r))
   and scisql_angSep(g1.ra, g1.dec, g2.ra, g2.dec) < 0.1