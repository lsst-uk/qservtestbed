/* Q1: Carole Mundell wants to be able to do forced-photometry on near-IR (e.g. VISTA) images at the positions of LSST sources that could be AGN */
/* Had to explicitly give the SQL for the reliablelaspointsource view and remove the sub-query. Spotted a problem with NOT LIKE */

SELECT mf.filename, f.shortname, l.ra, l.dec, obstype, frametype 
select mf.fileName,f.shortName,l.ra,l.decl,obstype,frameType from  lasSource as l,UKIDSSDR8.CurrentAstrometry as ca, 
UKIDSSDR8.Multiframe as mf, UKIDSSDR8.MultiframeDetector as mfd, UKIDSSDR8.Filter as f 
where mf.multiframeID=mfd.multiframeID and mf.multiframeID=ca.multiframeID and mfd.extNum=ca.extNum and f.filterID=mf.filterID  
and mf.fileName != 'NONE' AND ((l.ra >= minRA and l.ra <= maxRA) or (l.ra + 360.0 >= minRA and l.ra + 360.0 <= maxRA)) 
and ( l.decl >= minDec and l.decl <= maxDec) AND (priOrSec<=0 OR priOrSec=frameSetID) AND yClass = -1 AND yppErrBits = 0 
AND j_1Class = -1 AND j_1ppErrBits = 0 AND hClass = -1 AND hppErrBits = 0 AND (j_2Class=-1 OR j_2Class = -9999) 
AND j_2ppErrBits <= 0 AND (kClass=-1 OR kClass = -9999)  AND kppErrBits <= 0 AND yXi BETWEEN -1.0 AND +1.0 AND yEta BETWEEN -1.0 
AND +1.0 AND j_1Xi BETWEEN -1.0 AND +1.0 AND j_1Eta BETWEEN -1.0 AND +1.0 AND hXi BETWEEN -1.0 AND +1.0 AND hEta BETWEEN -1.0 
AND +1.0 AND ((kXi BETWEEN -1.0 AND +1.0 AND kEta BETWEEN -1.0 AND +1.0) OR kXi < -0.9e9) AND j_1mhpnt +hmkpnt > 2.5 and kapermag3 > 0 
and kapermag3 < 16.5 and obstype LIKE 'OBJECT' and frameType like '%stack'
