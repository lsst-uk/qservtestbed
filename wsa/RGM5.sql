/*Q5: Stephen Smartt wants information about sources from known catalogues to location of newly-discovered LSST transients to aid their classification.
Model this as extracting photometric and classification information from WSA for all sources around a certain source position.

*/
select ra,dec,ydeblend,yapermag3,yclass,ypperrbits,
j_1deblend,j_1apermag3,j_1class,j_1pperrbits,
hdeblend,hapermag3,hclass,hpperrbits,
kdeblend,kapermag3,kclass,kpperrbits
from lasSource where ra between 181.5 and 181.7
and dec between -0.7 and -0.5
and dbo.fgreatcircledist(181.6, -0.6, ra,dec)<=2
