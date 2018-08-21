/* Q1: Carole Mundell wants to be able to do forced-photometry on near-IR (e.g. VISTA) images at the positions of LSST sources that could be AGN */

SELECT mf.filename, f.shortname, l.ra, l.dec, obstype, frametype 
from  lassource as l,currentAstrometry as ca, Multiframe as mf, multiframeDetector as mfd, filter as f 
where mf.multiframeId=mfd.multiframeId and mf.multiframeId=ca.multiframeId and mfd.extNum=ca.extNum and f.filterid=mf.filterid and mf.filename != 'NONE' and 
((l.ra >= minRA and l.ra <= maxRA) or (l.ra + 360.0 >= minRA and l.ra + 360.0 <= maxRA)) and ( l.dec >= minDec and l.dec <= maxDec) and l.sourceid in (select  sourceid from reliablelaspointsource where j_1mhpnt +hmkpnt > 2.5 and kapermag3 > 0 and kapermag3 < 16.5)  and ( obstype NOT LIKE 'BIAS%' AND frametype NOT LIKE 'BIAS%' AND obstype NOT LIKE 'DARK%' AND frametype NOT LIKE 'DARK%' AND obstype NOT LIKE 'SKY%' AND frametype NOT LIKE 'SKY%' AND obstype NOT LIKE 'FOCUS%' AND frametype NOT LIKE 'FOCUS%' AND obstype NOT LIKE 'CONFIDENCE%' AND frametype NOT LIKE 'CONFIDENCE%' AND obstype NOT LIKE '%FLAT%' AND frametype NOT LIKE '%FLAT%')  and ( frametype LIKE '%stack%') order by l.ra, l.dec, mf.filterID,mf.dateobs
