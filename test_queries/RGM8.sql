/*Q8: Aleks Scholz wants lightcurves of stars to help detect stellar rotational periods.
Model this as extracting a lightcurve for a DXS source*/

SELECT m.mjdObs,d.aperMag3,d.aperMag3Err,d.ppErrBits,d.seqNum,x.flag
FROM dxsSourceXDetectionBestMatch AS x,dxsDetection AS d,Multiframe
AS m WHERE x.sourceID=446677639289 AND x.multiframeID=d.multiframeID
AND x.extNum=d.extNum AND x.seqNum=d.seqNum AND
x.multiframeID=m.multiframeID AND d.filterID=5 ORDER BY m.mjdObs
