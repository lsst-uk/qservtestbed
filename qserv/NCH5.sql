/* Extragalactic variables in DXS 
584 rows in 5s, 1 more row than SQL Server (rounding differences in kMag-kMin)
*/

SELECT kMaxMag-kMinMag,kMaxMag,kMinMag,s.sourceID,s.ra,s.decl,v.frameSetID,v.jmedianMag,v.jMagRms,v.jnGoodObs,v.jskewness,v.hmedianMag, 
v.hMagRms,v.hnGoodObs,v.hskewness, v.kmedianMag, v.kMagRms,v.knGoodObs,v.kskewness FROM UKIDSSDR8_dxsSource.dxsSource AS s, 
UKIDSSDR8.dxsVariability AS v  WHERE v.sourceID=s.sourceID AND s.mergedClass IN (-1.0,-2.0) AND v.variableClass=1 
AND (((jMaxMag-jMinMag)>0.1 AND jMinMag>0. AND jnGoodObs>=5) OR  ((hMaxMag-hMinMag)>0.1 AND hMinMag>0. AND hnGoodObs>=5) 
OR  ((kMaxMag-kMinMag)>0.1 AND kMinMag>0. AND knGoodObs>=5));
