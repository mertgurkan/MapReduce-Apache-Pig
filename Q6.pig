-- Load the data from HDFS and define the schema
coviddata = LOAD '/data/Covid19Canada.csv' USING PigStorage(',') AS (prname:CHARARRAY, idate:CHARARRAY, newcases:INT, newdeaths:INT, tests:INT, recoveries:INT);


QuebecOnly = FILTER coviddata BY prname == 'Quebec';

dataQuebec = FOREACH QuebecOnly GENERATE prname,idate,newdeaths;

aggquebec = FOREACH QuebecOnly GENERATE prname,newdeaths;

datagroup = GROUP aggquebec BY prname;

dataagg = FOREACH datagroup GENERATE group as prname,SUM(aggquebec.newdeaths) as death_total;

combineddata = JOIN dataQuebec BY prname, dataagg BY prname;

datafinal = FOREACH combineddata GENERATE idate, newdeaths, (newdeaths*1.0/death_total*1.0)*1.0 as ratio;

dataresult = FILTER datafinal BY ratio >= 0.01;

-- generate the output
DUMP dataresult;

