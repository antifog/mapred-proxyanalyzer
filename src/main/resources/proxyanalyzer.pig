REGISTER globant-hadoop-proxyanalyzer-1.0-SNAPSHOT.jar;
proxydata = LOAD '$inputPath' AS (line:chararray);
validrecs = FILTER proxydata BY (NOT (line matches '.*globant.*'));
parsedinfo = FOREACH validrecs GENERATE FLATTEN
(
	(tuple(double,chararray,double)) REGEX_EXTRACT_ALL(line, '.*size="(.*?)".*url="(?:http|https)://(.*?)/.*".*fullreqtime="(.*?)".*')
)
AS (size:double, domain:chararray, reqtime:double);
sitesgrouped = GROUP parsedinfo BY domain;
sitegroupinfo = FOREACH sitesgrouped GENERATE group as domain, COUNT($1.size) as visitscount, com.globant.training.hadoop.loganalyzer.helper.ScaledSize(AVG($1.size), '$scaleSize') as sizeAvg, AVG($1.reqtime) as reqtimeAvg;
infoordered = ORDER sitegroupinfo BY visitscount DESC;
totalresult = LIMIT infoordered 10;
STORE totalresult INTO '$outputPath' USING PigStorage(',');
