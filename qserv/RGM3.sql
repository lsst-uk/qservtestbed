
/* Aprajita Verma wants a sources with red centres and blue outskirts to aid search for double source plane lenses.
Maybe look at colours at different apertures. problems with select A-B as c : 237 rows in set (12.25 sec) */

select ra,decl,frameSetID,yAperMag3,j_1AperMag3,hAperMag3,kAperMag3,
(hAperMag6-kAperMag6)-(hAperMag3-kAperMag6),
(j_1AperMag3-yAperMag3)-(j_1AperMag6-yAperMag6)
 from UKIDSSDR8_lasSource.lasSource as s where
yClass=1 and j_1Class=1 and hClass=1 and kClass=1 and yppErrBits=0 and j_1ppErrBits=0
and hppErrBits=0 and kppErrBits=0
and (hAperMag6-kAperMag6)-(hAperMag3-kAperMag3) >1
and (j_1AperMag3-yAperMag3)-(j_1AperMag6-yAperMag6) > 1
and j_1AperMag3>0 and yAperMag3 >0 and j_1AperMag6 > 0 and yAperMag6 >0
and kAperMag3 > 0 and hAperMag3 >0 and kAperMag6 > 0 and hAperMag6 > 0
order by yAperMag3;
