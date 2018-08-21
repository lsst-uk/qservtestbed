/* Extragalactic variables in DXS */

SELECT s.sourceID,s.ra,s.dec,v.frameSetID, 
v.jMedianMag, v.jMagRms,v.jnGoodObs,v.jSkewness, (v.jMaxMag-v.jMinMag) AS jRange,
v.hMedianMag, v.hMagRms,v.hnGoodObs,v.hSkewness, (v.hMaxMag-v.hMinMag) AS hRange,
v.kMedianMag, v.kMagRms,v.knGoodObs,v.kSkewness, (v.kMaxMag-v.kMinMag) AS kRange
FROM dxsVariability AS v, dxsSource AS s 
/* join tables */
WHERE v.sourceID=s.sourceID AND
/* point source variables */
s.mergedClass IN (-1,-2) AND v.variableClass=1 AND
/* delta mag in > 0.1 in ANY filter, with at least 5 good obs in that filter */ 
(((jMaxMag-jMinMag)>0.1 AND jMinMag>0. AND jnGoodObs>=5) OR 
((hMaxMag-hMinMag)>0.1 AND hMinMag>0. AND hnGoodObs>=5) OR 
((kMaxMag-kMinMag)>0.1 AND kMinMag>0. AND knGoodObs>=5))
