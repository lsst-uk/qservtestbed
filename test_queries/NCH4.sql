/* Optical/infrared selection of QSO candidates (a bit pointless unless the qserv deployment has SDSS ?!) */

SELECT psfMag_i-psfMag_z AS imz,
psfMag_z-j_1AperMag3 AS zmj,
psfMag_i-yAperMag3 AS imy,
ymj_1Pnt AS ymj
FROM lasPointSource AS s,
lasSourceXDR7PhotoObj AS x,
BestDR7..PhotoObj AS p
WHERE
/* Join predicates: */
s.sourceID = x.masterObjID AND
x.slaveObjID = p.objID AND
x.distanceMins < 1.0/60.0 AND
/* Select only the nearest primary SDSS 
point source crossmatch: */
x.distanceMins IN (
SELECT MIN(distanceMins)
FROM lasSourceXDR7PhotoObj 
WHERE masterObjID = x.masterObjID AND
sdssPrimary = 1 AND
sdssType = 6
) AND
/* Remove any default SDSS mags: */
psfMag_i > 0.0 AND
/* Colour cuts for high-z QSOs from 
Hewett et al. (2006) and Venemans
et al. (2007): */
psfMag_i-yAperMag3 > 4.0 AND
ymj_1Pnt < 0.8 AND
psfMagErr_u > 0.3 AND
psfMagErr_g > 0.3 AND
psfMagErr_r > 0.3 
/* UKIDSS DR2 / SDSS DR5 rows returned: 12
Execution time: 19m 56s 

UKIDSS DR8 / SDSS DR7: 141 rows in 12m 42s
*/

