
/* 8 (query 034) - Search for merging galaxy pairs, as per the prescription in Allam et al. 2004 */

select g1.objID as Gal1_ID, g2.objID as Gal2_ID
from bestdr7..Galaxy g1, bestdr7..Neighbors n, bestdr7..Galaxy g2	   
where 
   g1.objID = N.objID     				                    
   and g2.objID = N.NeighborObjID
   and g1.objId < g2.ObjID
   and N.NeighborType = 3
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
   and (n.distance <= (g1.petroR50_r + g2.petroR50_r))