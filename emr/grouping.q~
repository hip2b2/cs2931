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
 clicked BOOLEAN )
PARTITIONED BY (
 day STRING,
 hour STRING )
STORED AS SEQUENCEFILE
LOCATION '${INPUT}/joined_impressions/';

-- make sure partitions are recognized
MSCK REPAIR TABLE raw_logs;

-- create table for output user agent
CREATE EXTERNAL TABLE IF NOT EXISTS user_agent_count (
 feature STRING,
 clicked_percent DOUBLE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/user_agent_count/';

-- process and write to table
INSERT OVERWRITE TABLE user_agent_count
SELECT
 temp.feature,
 sum(if(temp.clicked = 'true', 1, 0)) / cast(count(1) as DOUBLE) as clicked_percent
FROM (
 SELECT concat('ua:', trim(lower(ua.feature))) as feature, ua.ad_id, ua.clicked
 FROM (
  MAP raw_logs.user_agent, raw_logs.ad_id, raw_logs.clicked
  USING '${LIBS}/split_user_agent.py' as feature, ad_id, clicked
  FROM raw_logs
 ) ua
) temp
GROUP BY temp.feature;

-- create table for output ip
CREATE EXTERNAL TABLE IF NOT EXISTS ip_count (
 feature STRING,
 clicked_percent DOUBLE )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/ip_count/';

-- process and write to table
INSERT OVERWRITE TABLE ip_count
SELECT
 temp.feature,
 sum(if(temp.clicked = 'true', 1, 0)) / cast(count(1) as DOUBLE) as clicked_percent
FROM (
 SELECT concat('ip:', regexp_extract(ip_address, '^([0-9]{1,3}\.[0-9]{1,3}).*', 1)) as feature, ad_id, cast(clicked as STRING) as clicked
 FROM raw_logs
) temp
GROUP BY temp.feature;
