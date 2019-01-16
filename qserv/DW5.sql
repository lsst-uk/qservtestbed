/* 5 - Find the brightness of the closest source within ? arcmin (of each source?) (query 029)*/

SELECT s1.sourceID as Source1, s2.sourceID as Source2, s2.ra, s2.decl, s2.jAperMag3 as Source2_J_Mag

FROM        dxsSource_test1.dxsSource AS s1
LEFT JOIN   dxsSource_test1.dxsSourceNeighbours AS n ON s1.sourceID=n.masterObjID
JOIN        dxsSource_test1.dxsSource AS s2 ON s2.sourceID=n.slaveObjID

WHERE n.slaveObjID=(
    SELECT TOP 1 nn.slaveObjID
    FROM    dxsSource_test1.dxsSourceNeighbours AS nn
    JOIN    dxsSource_test1.dxsSource AS s3 ON (nn.slaveObjID=s3.sourceID)
    WHERE   nn.masterObjID = s1.sourceID
    AND     s3.jAperMag3>-100 
    ORDER BY scisql_angSep(s1.ra, s1.decl, s3.ra, s3.decl) ASC
)
AND scisql_angSep(s1.ra, s1.decl, s2.ra, s2.decl) < 30