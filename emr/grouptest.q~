-- Hive Script for Grouping Test Job
-- wyu@ateneo.edu

-- mounting data set for processing
CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
 request_begin_time STRING,
 ad_id STRING,
 impression_id STRING, 
 page STRING,
 user_agent STRING,
 user_cookie STRING,
 ip_address STRING,
 clicked STRING )
PARTITIONED BY (
 day STRING,
 hour STRING )
STORED AS SEQUENCEFILE
LOCATION '${INPUT}/joined_impressions/';

-- make sure partitions are recognized
MSCK REPAIR TABLE raw_logs;

-- create file for results by ua and put query results in it
CREATE EXTERNAL TABLE IF NOT EXISTS ua_summary (
 ua STRING,
 clicks INT,
 impressions INT )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/ua_summary/';

INSERT OVERWRITE TABLE ua_summary
SELECT 
  temp.ua as ua,
  sum(if(temp.clicked LIKE 'true', 1, 0)) as clicks, 
  count(temp.ad_id) as impressions
FROM 
  (SELECT 
     regexp_extract(raw_logs.user_agent, '^([A-Za-z]*/[0-9]*\.[0-9]*)\ .*', 1) as ua, 
     raw_logs.ad_id, 
     raw_logs.clicked
   FROM 
     raw_logs) temp
GROUP BY temp.ua
ORDER BY impressions DESC;

-- create file for results by ip and put query results in it
CREATE EXTERNAL TABLE IF NOT EXISTS ip_summary (
 ip STRING,
 clicks INT,
 impressions INT )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/ip_summary/';

INSERT OVERWRITE TABLE ip_summary
SELECT 
  temp.ip as ip,
  sum(if(temp.clicked LIKE 'true', 1, 0)) as clicks, 
  count(temp.ad_id) as impressions
FROM 
  (SELECT 
     regexp_extract(raw_logs.ip_address, '^([0-9]{1,3}\.[0-9]{1,3}).*', 1) as ip, 
     raw_logs.ad_id, 
     raw_logs.clicked 
   FROM 
     raw_logs) temp
GROUP BY temp.ip
ORDER BY impressions DESC;

-- create file for results by ad and put query results in it
CREATE EXTERNAL TABLE IF NOT EXISTS ad_summary (
 ad_id STRING,
 clicks INT,
 impressions INT )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/ad_summary/';

INSERT OVERWRITE TABLE ad_summary
SELECT 
  raw_logs.ad_id, 
  sum(if(raw_logs.clicked LIKE 'true', 1, 0)) as clicks, 
  count(raw_logs.ad_id) as impressions
FROM raw_logs
GROUP BY raw_logs.ad_id
ORDER BY impressions DESC;
