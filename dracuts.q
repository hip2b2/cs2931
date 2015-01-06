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
  FIELDS TERMINATED BY '32'
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost/user/wyy/dracut';

SELECT filename FROM dracut_logs;
