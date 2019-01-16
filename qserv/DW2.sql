/* 2. (query 007) - Select time series data for all objects in a given area of the sky, in a given photometric band (J, in our case) with a given variability index (>0.8) */

SELECT s.sourceID, v.jmeanMag, m.mjdObs, d.aperMag3, d.aperMag3err

FROM dxsSource_test1.dxsSourceXDetectionBestMatch AS x,UKIDSSDR8_dxsDetection.dxsDetection AS d, UKIDSSDR8.Multiframe AS m, dxsSource_test1.dxsVariability AS v, dxsSource_test1.dxsSource AS s

WHERE x.sourceID=s.sourceID 
AND x.multiframeID=d.multiframeID 
AND x.extNum=d.extNum 
AND x.seqNum=d.seqNum 
AND x.multiframeID=m.multiframeID 
AND d.filterID=3 
AND v.sourceID=s.sourceID 
AND v.jprobVar > 0.8
AND d.aperMag3 > -100
AND scisql_angSep(d.ra, d.decl,334.25,0.3) < 0.5

AND scisql_angSep(d.ra, d.decl,s.ra,s.decl) < 0.1

ORDER BY s.sourceID, m.mjdObs