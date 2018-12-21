/* Q6: Vasiliy Belukurov wants to be able to select stellar tracers of Galactic structure in (u-g,g-r) space.
Model this as a selection of stellar objects from the LAS (employing definition of lasPointSource view)  in some region of colour-colour space.
189 rows in set (14.19 sec)
*/

SELECT ra,decl,yAperMag3,j_1Apermag3,hAperMag3 FROM   UKIDSSDR8_lasSource.lasSource WHERE  (priOrSec <= 0 OR priOrSec = frameSetID) 
AND yClass   BETWEEN -2.0 AND -1.0 AND yppErrBits < 256  AND    j_1Class BETWEEN -2.0 AND -1.0 AND j_1ppErrBits < 256 
AND hClass   BETWEEN -2.0 AND -1.0 AND hppErrBits < 256 AND     (j_2Class BETWEEN -2.0 AND -1.0 OR j_2Class = -9999) 
AND (j_2ppErrBits < 256) AND    (kClass   BETWEEN -2.0 AND -1.0 OR kClass = -9999) AND (kppErrBits   < 256) 
AND yAperMag3-j_1Apermag3 < 0 AND abs(j_1AperMag3 - j_2AperMag3 ) < 0.05;
