/* Get the most flaring stars ordered by number of candidate flares 

Got to the query below but then hit a bug with Qserv not allowing more than one attribute in the group by.
Reported to devs.

*/
SELECT v.sourceID as vID, s.ra as sra, s.decl as sdecl, v.framesetID as
vFSID, knGoodObs as gknGoodObs, kMinMag as gkMinMag, kmedianMag as
gkmedianMag, kMaxMag as gkMaxMag, variableClass as gvariableClass,
mergedClass as gmergedClass, count(*) as nBrightDetections FROM
UKIDSSDR8_dxsSource.dxsSource AS s, UKIDSSDR8.dxsSourceXDetectionBestMatch
AS b, UKIDSSDR8.dxsVariability AS v, UKIDSSDR8_dxsDetection.dxsDetection as
d  WHERE s.sourceID=v.sourceID AND b.sourceID=v.sourceID AND
b.multiframeID=d.multiframeID  AND b.extNum=d.extNum AND b.seqNum=d.seqNum
and scisql_angSep(s.ra, s.decl, d.ra, d.decl) < 0.01 and kmedianMag<19. and
kmedianMag>0. AND knGoodObs>=5 AND kbestAper=5 AND (kmedianMag-kMinMag)>0.5
AND kMinMag>0. AND d.seqNum>0 AND d.ppErrBits IN (0,16) AND d.filterID=5 AND
d.aperMag5>0 AND d.aperMag5<(kmedianMag-0.5) group by vID,  sra, sdecl,
vFSID, gknGoodObs, gkMinMag, gkmedianMag, gkMaxMag, gvariableClass,
gmergedClass;
