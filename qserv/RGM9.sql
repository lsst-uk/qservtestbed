/*
Q9: Eamonn Kerins wants to detect possible exoplanet microlensing events that brighten by more than 3 mags on timescales above 30 days.
Model this with a query on the dxsVariability table
To get this to work it needs the scisql_angSep which means it's not really using the neighbour table properly. 
7 rows in set (11 min 13.24 sec)
*/
 
SELECT x.sourceID AS xid,min(d.aperMag3),max(d.aperMag3),count(*) FROM UKIDSSDR8_dxsSource.dxsSource AS s, 
UKIDSSDR8.dxsSourceXDetectionBestMatch AS x, UKIDSSDR8.dxsVariability AS v, UKIDSSDR8_dxsDetection.dxsDetection as d 
WHERE s.sourceID=v.sourceID AND s.mergedClass=-1.0 AND v.variableClass=1 and abs(kMinMag-kMaxMag) >3 and x.sourceID=s.sourceID 
and x.multiframeID=d.multiframeID AND x.extNum=d.extNum AND x.seqNum=d.seqNum and scisql_angSep(s.ra, s.decl, d.ra, d.decl) < 0.01 
and d.aperMag3 > 0 GROUP BY xid;

/*
SELECT x.sourceID,d.aperMag3 FROM UKIDSSDR8_dxsSource.dxsSource AS s, UKIDSSDR8.dxsSourceXDetectionBestMatch AS x, 
UKIDSSDR8.dxsVariability AS v, UKIDSSDR8_dxsDetection.dxsDetection as d WHERE s.sourceID=v.sourceID 
AND s.mergedClass=-1.0 AND v.variableClass=1 and abs(kMinMag-kMaxMag) >3 and x.sourceID=s.sourceID 
and x.multiframeID=d.multiframeID AND x.extNum=d.extNum AND x.seqNum=d.seqNum and scisql_angSep(s.ra, s.decl, d.ra, d.decl) < 0.01 
and d.aperMag3 > 0 ;

138 rows in set (11 min 3.24 sec)

The SQL server query groups by sourceID to get min/max mags but adding the group by clause etc to make it 

SELECT x.sourceID,min(d.aperMag3),max(d.aperMag3),count(*) FROM UKIDSSDR8_dxsSource.dxsSource AS s, 
UKIDSSDR8.dxsSourceXDetectionBestMatch AS x, UKIDSSDR8.dxsVariability AS v, UKIDSSDR8_dxsDetection.dxsDetection as d 
WHERE s.sourceID=v.sourceID AND s.mergedClass=-1.0 AND v.variableClass=1 and abs(kMinMag-kMaxMag) >3 
and x.sourceID=s.sourceID and x.multiframeID=d.multiframeID AND x.extNum=d.extNum AND x.seqNum=d.seqNum 
and scisql_angSep(s.ra, s.decl, d.ra, d.decl) < 0.01 and d.aperMag3 > 0 group by x.sourceID;

This parses/runs but eventaully comes back with
ERROR 4120 (Proxy): Unable to return query results:
Failure while merging result

*/
