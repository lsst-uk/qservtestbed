/* 1 - Multiple joins from several tables (query 070): */

SELECT
    o.uFlux_PS, o.uFlux_PS_Sigma, o.gFlux_PS, o.gFlux_PS_Sigma,
    o.rFlux_PS, o.rFlux_PS_Sigma, o.iFlux_PS, o.iFlux_PS_Sigma,
    o.zFlux_PS, o.zFlux_PS_Sigma, o.yFlux_PS, o.yFlux_PS_Sigma,
    ce.visit, ce.raft, ce.ccd,
    f.filterName,
    scisql_dnToAbMag(fs.flux, ce.fluxMag0) AS forcedPsfMag,
    scisql_dnToAbMagSigma(fs.flux, fs.fluxSigma, ce.fluxMag0, ce.fluxMag0Sigma) AS forcedPsfMagErr,
    so.uMag, so.gMag, so.rMag, so.iMag, so.zMag, so.yMag

FROM
    Object                    AS o
    JOIN ForcedSource         AS fs ON (fs.objectId = o.objectId)
    JOIN Science_Ccd_Exposure AS ce ON (ce.scienceCcdExposureId = s.scienceCcdExposureId)
    JOIN RefObjMatch          AS om ON (om.objectId = o.objectId)
    JOIN SimRefObject         AS so ON (om.refObjectId = so.refObjectId)
    JOIN Filter               AS f  ON (f.filterId = ce.filterId)

WHERE
     fs.fluxSigma > 0
 AND om.nObjMatches = 1
 AND NOT (fs.flagNegative | fs.flagPixEdge | fs.flagPixSaturAny | 
          fs.flagPixSaturCen | fs.flagBadApFlux | fs.flagBadPsfFlux);
