-- This script reports the total number of deaths per province from highest to lowest

--load the data from HDFS and define the schema
coviddata = LOAD '/data/Covid19Canada.csv' USING PigStorage(',') AS (prname:CHARARRAY, idate:CHARARRAY, newcases:INT, newdeaths:INT, tests:INT, recoveries:INT);

deathperprovince = FOREACH coviddata GENERATE prname,newdeaths;

datacluster = GROUP deathperprovince BY prname;

dataagg = FOREACH datacluster GENERATE  group,SUM(deathperprovince.newdeaths) as total_Deaths;

optimizedata = FILTER dataagg BY total_Deaths >= 100;
 
ordereddata = ORDER optimizedata BY total_Deaths DESC;

-- print result
DUMP ordereddata;




