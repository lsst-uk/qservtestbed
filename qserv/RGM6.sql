/* Q6: Vasiliy Belukurov wants to be able to select stellar tracers of Galactic structure in (u-g,g-r) space.
Model this as a selection of stellar objects from the LAS (employing definition of lasPointSource view)  in some region of colour-colour space.
Added a limit as this is returning too many rows and need to add a colour cut


*/
SELECT ra,decl FROM   lasSource WHERE  (priOrSec <= 0 OR priOrSec = frameSetID) AND    yClass   BETWEEN -2 AND -1 AND yppErrBits < 256 
AND    j_1Class BETWEEN -2 AND -1 AND j_1ppErrBits < 256 AND    hClass   BETWEEN -2 AND -1 AND hppErrBits < 256 AND    
(j_2Class BETWEEN -2 AND -1 OR j_2Class = -9999) AND    (j_2ppErrBits < 256) AND    (kClass   BETWEEN -2 AND -1 OR kClass = -9999)  
AND    (kppErrBits   < 256) limit 1000;
