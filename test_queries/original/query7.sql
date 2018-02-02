/* 7 - Find stars with stellar neighbor within distance x where at least one of the stellar neighbors has the colors of a white dwarf (query 043): */

SELECT  S1.objectId AS s1, 
        S2.objectId AS s2
FROM    Object S1,                                   -- S1 is the white dwarf
        Object S2                                    -- S2 is the second star
WHERE   S1.extendedParam < 0.2                       -- is star
   AND  S2.extendedParam < 0.2                       -- is star
   AND  spDist(S1.ra, S1.decl, S2.ra, S2.decl) < .05 -- the 5 arcsecond test
   AND  S1.uMag-S1.gMag < 0.4                        -- and S1 meets Paul Szkody's color cut
   AND  S1.gMag-S1.rMag < 0.7                        -- for white dwarfs
   AND  S1.rMag-S1.iMag > 0.4 
   AND  S1.iMag-S1.zMag > 0.4
