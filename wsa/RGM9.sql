/*Q9: Eamonn Kerins wants to detect possible exoplanet microlensing events that brighten by more than 3 mags on timescales above 30 days.
Model this with a query on the dxsVariability table*/

SELECT x.sourceid,min(d.apermag3),max(d.apermag3),count(*)
FROM dxsSourceXDetectionBestMatch AS x,dxsDetection AS d,Multiframe AS m
WHERE  x.multiframeID=d.multiframeID AND x.extNum=d.extNum AND x.seqNum=d.seqNum AND x.multiframeID=m.multiframeID AND d.filterID=5 
and x.sourceid in (
SELECT v.sourceID
FROM dxsSource AS s,dxsVariability AS v
WHERE s.sourceID=v.sourceID AND s.mergedClass=-1 AND v.variableClass=1
and abs(kMinMag-kMaxMag) >3 ) and d.apermag3 >0
group by x.sourceid
order by x.sourceid
