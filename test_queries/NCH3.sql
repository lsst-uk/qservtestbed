/* Scale-out: counts-in-cells in the GPS (see e.g. WSA paper) */

SELECT CAST(ROUND(l*6.0,0) AS INT)/6.0 AS lon, 
CAST(ROUND(b*6.0,0) AS INT)/6.0 AS lat,
COUNT(*) AS num
FROM gpsSource
WHERE k_1Class BETWEEN -2 AND -1 AND 
k_1ppErrBits < 256 AND
/* Make a seamless selection (i.e. exclude 
duplicates) in any overlap regions: */
(priOrSec=0 OR priOrSec=frameSetID)
/* Bin up in 10 arcmin x 10 arcmin cells: */
GROUP BY CAST(ROUND(l*6.0,0) AS INT)/6.0,
CAST(ROUND(b*6.0,0) AS INT)/6.0
/* UKIDSS DR2 rows returned: 28,026
Execution time: 72m 00s 

UKIDSS DR8+: Query returned 55024 result rows in 2m 47s
*/
