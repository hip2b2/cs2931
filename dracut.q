-- Hive Script for Processing dracut.log
-- wyu@ateneo.edu

DROP TABLE IF EXISTS dracut_logs;
CREATE EXTERNAL TABLE IF NOT EXISTS dracut_logs (
  weekday STRING,
  month STRING,
  day STRING, 
  time STRING,
  timezone STRING,
  year STRING,
  info STRING,
  action STRING,
  filename STRING
) 
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION 'hdfs://localhost/user/wyy/dracut/';

DROP TABLE IF EXISTS dracut_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS dracut_summary (
 file STRING,
 hits INT )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost/user/wyy/dracut_summary/';

INSERT OVERWRITE TABLE dracut_summary
SELECT 
  temp.file as file,
  count(temp.file) as hits
FROM 
  (SELECT 
     regexp_extract(dracut_logs.filename, '^.*/([A-Za-z0-9.-]*)\'$', 1) as file 
   FROM 
     dracut_logs) temp
GROUP BY temp.file
ORDER BY hits, file DESC;
