/* Get the most flaring stars ordered by number of candidate flares */

SELECT v.sourceID,s.ra, s.dec,
/* select some useful attributes, pointing info, number of observations, min, medium, maximum, variable class, and star/galaxy class */
v.framesetID, knGoodObs, kMinMag, kMedianMag, kMaxMag, variableClass, mergedClass, (kMedianMag-kMinMag) as kFlareMag,
count(*) as nBrightDetections
FROM dxsVariability as v, dxsSource as s, dxsSourceXDetectionBestMatch as b,
dxsDetection as d
WHERE s.sourceID=v.sourceID AND b.sourceID=v.sourceID AND b.multiframeID=d.multiframeID 
AND b.extNum=d.extNum AND b.seqNum=d.seqNum AND
/* (23108158) select the magnitude range, brighter than Ks=17 and not default. */
kMedianMag<19. and kMedianMag>0. AND
/* (4684005) at least 5 observations */
knGoodObs>=5 AND kBestAper=5 AND
/* (403628) Min mag is at least 2 magnitudes brighter than median mag(but minMag is not default) */ 
(kMedianMag-kMinMag)>0.5 AND kMinMag>0. AND
/* (881) Only good K band detections in same aperture as statistics are calculated in*/ 
d.seqNum>0 AND d.ppErrBits IN (0,16) AND d.filterID=5 AND d.aperMag5>0 AND d.aperMag5<(kMedianMag-0.5)
/* Group detections */
GROUP BY v.sourceID, s.ra, s.dec,
v.framesetID, knGoodObs, kMinMag, kMedianMag, kMaxMag, variableClass, mergedClass
HAVING COUNT(*)>2
/* Order by largest change in magnitude first.*/ 
ORDER BY count(*) DESC
