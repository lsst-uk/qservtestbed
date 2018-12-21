/*Q8: Aleks Scholz wants lightcurves of stars to help detect stellar rotational periods.
Model this as extracting a lightcurve for a DXS source : 44 rows in set (0.32 sec) */

SELECT m.mjdObs,d.aperMag3,d.aperMag3Err,d.ppErrBits,d.seqNum,x.flag FROM UKIDSSDR8_dxsDetection.dxsDetection AS d, 
UKIDSSDR8.dxsSourceXDetectionBestMatch AS x, UKIDSSDR8.Multiframe AS m WHERE x.sourceID=446677639289 AND x.multiframeID=d.multiframeID 
AND x.extNum=d.extNum AND x.seqNum=d.seqNum  AND x.multiframeID=m.multiframeID AND d.filterID=5 ORDER BY mjdObs;

