/*Q5: Stephen Smartt wants information about sources from known catalogues to location of newly-discovered LSST transients to aid their classification.
Model this as extracting photometric and classification information from WSA for all sources around a certain source position.
26 rows in set (0.41 sec)
*/
select ra,decl,yDeblend,yAperMag3,yClass,yppErrBits, j_1Deblend,j_1AperMag3,j_1Class,j_1ppErrBits,hDeblend,hAperMag3,hClass,hppErrBits,
kDeblend,kAperMag3,kClass,kppErrBits 
from UKIDSSDR8_lasSource.lasSource where qserv_areaspec_circle(181.6,-0.6,0.0333333);
