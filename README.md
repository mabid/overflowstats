overflowstats
=============

MapReduce jobs for trend calculation on Stackoverflow data

Data dump has been taken from Stack Exchange Network.

https://archive.org/details/stackexchange

map reduce jobs to calculate trends of tags on stackoverflow questions. This was about 30GB of data. I used Amazon's Elastic Map Reduce(EMR) to run the jobs and calculated two interesting trends. 


1) A trend for number of questions asked for a given tag over time

2) A trend for average time to an accepted answer after a question was asked for each tag.

A demo can be seen at http://overflowstats.herokuapp.com

Code for the front-end app is at http://github.com/mabid/overflow_front
