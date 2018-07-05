/*Q10: Manda Banerji wants to look for candidate high-z galaxies and quasars that are present in near-IR surveys but absent from LSST. 
Model this as a query for UKIDSS sources without SDSS cross-neighbours*/

select ra ,dec, framesetid, yapermag3,j_1apermag3,hapermag3,kapermag3,mergedclass from lassource as s 
where ra between 325 and 355 and dec between -1 and -0.5 
and sourceid not in (select masterobjid from lasSourceXDR7PhotoObjAll )
and yapermag3>0 and j_1apermag3>0 and hapermag3 >0
and kapermag3>0 and mergedclass=-1
