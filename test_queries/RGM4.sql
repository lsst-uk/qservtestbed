/* Q4: Benjamin Joachimi wants to be able to compare photo-z estimates from different surveys
Model this as selecting photo-z estimates for SDSS objects with photo-z estimates computed two ways. Add in a redshift cut, to reduce the numbers returned
*/

select  pz1.z, pz1.zerr, pz2.z, pz2.zerr from bestdr8..photoz as pz1, bestdr8..photozrf as pz2 where pz1.objid=pz2.objid and pz1.z > 0.3 and pz1.z < 0.4
