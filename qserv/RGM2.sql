
/* Model this as a query for filenames for matching WSA images in different bands around positions of sources 
selected according to a cut on ellipticity. 
This is, obviously, very similar to Q1, but require images in Y, H and K for colour image.*/
/*Higher ellipticities pulls out diffraction spikes, this mainly pulls out elliptical galaxies*/

Select s.ra as ra,s.decl as decl,y.fileName as yFile, l.yeNum as yExtNum from UKIDSSDR8_lasSource.lasSource as s, 
UKIDSSDR8.lasMergeLog as l, UKIDSSDR8.Multiframe as y, UKIDSSDR8.Multiframe as h, UKIDSSDR8.Multiframe as k  
where yEll between  0.8 and 0.85 and hEll between 0.8 and 0.85 and kEll between 0.8 and 0.85 and  s.frameSetID=l.frameSetID 
and y.multiframeID=ymfID and h.multiframeID=hmfID and k.multiframeID=kmfID;
