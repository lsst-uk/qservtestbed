/* 9 - Select all variable objects in clusters (query 048): 
This query was suggested by the LSST Science Collaboration (Supernovae). Assumes presence of Neighbors table.
*/

SELECT objectId
FROM   (SELECT v.objectId AS objectId, 
               COUNT(n.neighborObjectId) AS neighbors
        FROM   VarObject v
        JOIN   Neighbors n USING (objectId)
        WHERE  n.distance < :clusterRadiusThreshold
        GROUP BY v.objectId) AS C
WHERE  neighbors > :clusterDensityThreshold

