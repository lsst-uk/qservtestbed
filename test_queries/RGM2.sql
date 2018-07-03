
/* Model this as a query for filenames for matching WSA images in different bands around positions of sources 
selected according to a cut on ellipticity. 
This is, obviously, very similar to Q1, but require images in Y, H and K for colour image.*/
/*Higher ellipticities pulls out diffraction spikes, this pulls out elliptical galaxies*/


select s.ra,s.dec, 
y.filename as yFile, l.yenum,
h.filename as hFile, l.henum,
k.filename as kFile, l.kenum 
from lassource as s, lasmergelog as l,
multiframe as y, multiframe as h, multiframe as k 
where yell between  0.8 and 0.85 and hell between 0.8 and 0.85
and kell between 0.8 and 0.85
and s.framesetid=l.framesetid and 
y.multiframeid=ymfid and
h.multiframeid=hmfid and
k.multiframeid=kmfid
