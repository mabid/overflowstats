overflowstats
=============

MapReduce jobs for trend calculation on Stackoverflow data


map reduce jobs to calculate trends of tags on stackoverflow questions. This was about 30GB of data. I used Amazon's Elastic Map Reduce(EMR) to run the jobs and calculated two interesting trends. A trend for number of questions asked for a given tag over time, and a trend for average time to an accepted answer after a question was asked for each tag. A demo can be seen at \href{overflowstats.herokuapp.com}{overflowstats.herokuapp.com}, the code is available at \href{github.com/mabid/overflowstats}{github.com/mabid/overflowstats}
