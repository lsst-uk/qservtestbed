/* 8 (query 034) - Search for merging galaxy pairs, as per the prescription in Allam et al. 2004 */

SELECT g1.sourceID, g2.sourceID, n.distanceMins
FROM dxsSource AS g1, dxsSourceNeighbours AS n, dxsSource AS g2 
WHERE (g1.sourceID = n.masterObjID) 
AND (g2.sourceID = n.slaveObjID)
AND g1.sourceID < g2.sourceID
AND g1.mergedClass = 1 -- same as pGalaxy>90?
AND g2.mergedClass = 1
ORDER BY n.distanceMins
