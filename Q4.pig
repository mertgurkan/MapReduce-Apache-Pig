-- This script reports when the number of recoveries were at least twice as much as the number of cases reported on that day.

--load the data from HDFS and define the schema
coviddata = LOAD '/data/Covid19Canada.csv' USING PigStorage(',') AS (prname:CHARARRAY, idate:CHARARRAY, newcases:INT, newdeaths:INT, tests:INT, recoveries:INT);

recoverynumber = FILTER coviddata BY recoveries >= 50 AND newcases >= 50;

ratiocase = FOREACH recoverynumber GENERATE idate, newcases, recoveries, (recoveries*1.0/newcases*1.0)*1.0 as ratio;

twicenumber = FILTER ratiocase BY ratio > 2.0;

filteredresult = ORDER twicenumber BY idate;

-- output
DUMP filteredresult;
