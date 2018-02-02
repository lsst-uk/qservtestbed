/* 2. (query 007) - Select time series data for all objects in a given area of the sky, in a given photometric band (J, in our case) with a given variability index (>0.8) */

SELECT s.sourceID, v.jmeanMag, m.mjdObs, d.aperMag3, d.aperMag3err

FROM dxsSourceXDetectionBestMatch AS x, dxsDetection
AS d, Multiframe AS m, dxsVariability AS v, dxsSource AS s

WHERE x.sourceID=s.sourceID 
AND x.multiframeID=d.multiframeID 
AND x.extNum=d.extNum 
AND x.seqNum=d.seqNum 
AND x.multiframeID=m.multiframeID 
AND d.filterID=3 
AND v.sourceID=s.sourceID 
AND v.jprobVar > 0.8
AND d.aperMag3 > -100

ORDER BY s.sourceID, m.mjdObs
