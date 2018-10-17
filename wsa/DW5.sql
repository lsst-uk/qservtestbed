-- 5 - Find the brightness of the closest source within ? arcmin (of each source?) (query 029)

SELECT s1.sourceID, s2.sourceID, s2.ra, s2.dec, s2.jAperMag3

FROM        dxsSource AS s1
LEFT JOIN   dxsSourceNeighbours AS n ON s1.sourceID=n.masterObjID
JOIN        dxsSource AS s2 ON s2.sourceID=n.slaveObjID

WHERE n.slaveObjID=(
    SELECT TOP 1 nn.slaveObjID
    FROM    dxsSourceNeighbours AS nn
    JOIN    dxsSource AS s3 ON (nn.slaveObjID=s3.sourceID)
    WHERE   nn.masterObjID = s1.sourceID
    AND     s3.jAperMag3>-100 --Ignore default mags, only pick realistic sources
    ORDER BY s3.jAperMag3
)

-- THIS QUERY ACTUALLY LOOKS LIKE ITS FINDING THE BRIGHTEST SOURCE WITHIN
-- THE STANDARD REGION FROM NEIGHBOUR TABLE...

/*SELECT TOP 10
          o.ra, o.decl, o.flags, o.type, o.objid,
          o.psfMag_g, o.psfMag_r, o.psfMag_i, o.gMag, o.rMag, o.iMag, 
          o.petroRad_r, 
          o.q_g, o.q_r, o.q_i, 
          o.u_g, o.u_r, o.u_i, 
          o.mE1_r, o.mE2_r, o.mRrCc_r, o.mCr4_r, 
          o.isoA_r, o.isoB_r, o.isoAGrad_r, o.isoBGrad_r, o.isoPhi_r, 
          n.distance, p.r, p.g
FROM      Object AS o
LEFT JOIN Neighbors as n on o.objid=n.objid
JOIN      Object p ON (p.objId = n.neighborObjId)
WHERE     (o.ra > 120) and (o.ra < 240) 
    AND   (o.r > 16.) and (o.r<21.0) 
    AND   n.neighborObjId = (
               SELECT TOP 1 nn.neighborObjId
               FROM   Neighbors nn
               JOIN   Object pp ON (nn.neighborObjId = pp.objectId)
               WHERE  nn.objectId = o.objectId 
               ORDER BY pp.r
                          )
                          */
