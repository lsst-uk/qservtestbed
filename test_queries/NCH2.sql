/* Get all passband light curves for the above */

select s.sourceID,d.filterID,mjdObs,aperMag3
from dxsDetection d, dxsSource s, dxsSourceXDetectionBestMatch x, Multiframe m
where s.sourceID = x.sourceID and x.multiframeID = d.multiframeID and x.extNum = d.extNum
and x.seqNum = d.seqNum and m.multiframeID = x.multiframeID and d.aperMag3 > 0 
and s.sourceID in (
SELECT v.sourceID
FROM dxsVariability as v, dxsSource as s, dxsSourceXDetectionBestMatch as b,
dxsDetection as d
WHERE s.sourceID=v.sourceID AND b.sourceID=v.sourceID AND b.multiframeID=d.multiframeID 
AND b.extNum=d.extNum AND b.seqNum=d.seqNum AND
/* select the magnitude range, brighter than Ks=17 and not default. */
kMedianMag<19. and kMedianMag>0. AND
/* at least 5 observations */
knGoodObs>=5 AND kBestAper=5 AND
/* Min mag is at least 2 magnitudes brighter than median mag(but minMag is not default) */ 
(kMedianMag-kMinMag)>0.5 AND kMinMag>0. AND
/* Only good K band detections in same aperture as statistics are calculated in*/ 
d.seqNum>0 AND d.ppErrBits IN (0,16) AND d.filterID=5 AND d.aperMag5>0 AND d.aperMag5<(kMedianMag-0.5)
/* Group detections */
GROUP BY v.sourceID, s.ra, s.dec,
v.framesetID, knGoodObs, kMinMag, kMedianMag, kMaxMag, variableClass, mergedClass
HAVING COUNT(*)>2
)
order by s.sourceID,d.filterID,mjdObs
