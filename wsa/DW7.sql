/* 7 - Find stars with stellar neighbor within distance x where at least one of the stellar neighbors has the colors of a white dwarf (query 043): */

SELECT  S1.objID AS WD, 
        S2.objID AS Star
FROM    bestdr7..PhotoObjAll S1,                                   
        bestdr7..PhotoObjAll S2                                    
WHERE   S1.type =  6  
   AND  S2.type =  6 
   AND  dbo.fGreatCircleDist(S1.ra, S1.dec, S2.ra, S2.dec)*60 < 5
   AND  S1.u-S1.g < 0.4                       
   AND  S1.g-S1.r < 0.7                        
   AND  S1.r-S1.i > 0.4 
   AND  S1.i-S1.z > 0.4
   AND  S1.objID <> S2.objID
