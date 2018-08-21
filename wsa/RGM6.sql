/* Q6: Vasiliy Belukurov wants to be able to select stellar tracers of Galactic structure in (u-g,g-r) space.
Model this as a selection of stellar objects from the LAS (employing definition of lasPointSource view)  in some region of colour-colour space.
*/

SELECT ra,dec
FROM   lasSource
/*     Seamless selection of unique objects: */
WHERE  (priOrSec <= 0 OR priOrSec = frameSetID)
/*     Good quality, likely point source detections in Y, J1 and H: */
AND    yClass   BETWEEN -2 AND -1 AND yppErrBits < 256
AND    j_1Class BETWEEN -2 AND -1 AND j_1ppErrBits < 256
AND    hClass   BETWEEN -2 AND -1 AND hppErrBits < 256
/*     Good quality, likely point detection, OR no detection, in J2 and K: */
AND    (j_2Class BETWEEN -2 AND -1 OR j_2Class = -9999)
AND    (j_2ppErrBits < 256)
AND    (kClass   BETWEEN -2 AND -1 OR kClass = -9999)
AND    (kppErrBits   < 256)
