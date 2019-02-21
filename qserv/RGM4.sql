/* Q4: Benjamin Joachimi wants to be able to compare photo-z estimates from different surveys
Model this as selecting photo-z estimates for SDSS objects with photo-z estimates computed two ways. Add in a redshift cut, to reduce the numbers returned

select  pz1.z, pz1.zerr, pz2.z, pz2.zerr from bestdr8..photoz as pz1, bestdr8..photozrf as pz2 
where pz1.objid=pz2.objid and pz1.z > 0.3 and pz1.z < 0.4

currently BestDR7 has been loaded into Qserv as this was what had been paired with UKIDSSDR8. BestDR7 does not have photozrf, so for now 
use photoz twice.

37,898,187 in 6 minutes

*/
select pz1.z as z1,pz1.zErr as zErr1, pz2.z as z2,pz2.zErr as zErr2 from BestDr7.Photoz as pz1,  BestDr7.Photoz as pz2 
where pz1.objID=pz2.objID and pz1.z > 0.3 and pz1.z < 0.4

/* select  pz1.z, pz1.zerr, pz2.z, pz2.zerr from bestdr8..photoz as pz1, bestdr8..photozrf as pz2 where pz1.objid=pz2.objid and pz1.z > 0.3 and pz1.z < 0.4*/
