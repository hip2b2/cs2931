/* pig script test dracut */
/* wyu@ateneo.edu */

/* actual query */
A = LOAD 'hdfs://localhost/user/wyy/dracut/dracut.log' USING PigStorage(' '); 
B = FOREACH A GENERATE REGEX_EXTRACT($8, '^.*/([A-Za-z0-9.-]*)\'$', 1) AS file; 
C = GROUP B BY file;
D = FOREACH C GENERATE FLATTEN($0) as filegroup, COUNT(B) as count;
E = ORDER D BY count,filegroup DESC;
STORE E INTO 'hdfs://localhost/user/wyy/dracut_summary.log' USING PigStorage('|');
