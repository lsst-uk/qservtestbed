/*Aprajita Verma wants a sources with red centres and blue outskirts to aid search for double source plane lenses.
Maybe look at colours at different apertures
*/
select  ra,dec,framesetid,yapermag3, j_1apermag3, hapermag3,kapermag3,
(hapermag6-kapermag6)-(hapermag3-kapermag3) as rednessOuter,
(j_1apermag3-yapermag3)-( j_1apermag6-yapermag6) as bluenessCentre
from lassource as s where yclass=1 and j_1class=1 and hclass=1 and kclass=1 and ypperrbits=0 and j_1pperrbits=0 and hpperrbits=0 and kpperrbits=0 
and (hapermag6-kapermag6)-(hapermag3-kapermag3) >1
and (j_1apermag3-yapermag3)-( j_1apermag6-yapermag6) >1
and j_1apermag3 >0 and yapermag3 >0 and j_1apermag6 >0 and  yapermag6 >0
and kapermag3 >0 and hapermag3 >0 and  kapermag6 >0 and  hapermag6 >0
order by yapermag3
