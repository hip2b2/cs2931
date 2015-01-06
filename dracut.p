/* pig script test dracut */
/* wyu@ateneo.edu */

/*
Make sure you have the dependencies ready for this and run it afterwards.
1. Install pig. You can do this by typing 'yum install pig'
2. Create the directory for the raw dracut logs needed in this example as the appropriate user. In this case, it is the wyy user. 'hadoop fs -mkdir /user/wyy/dracut'
3. Upload the raw dracut.log file to the proper location as the appropriate user. In this case, it is the wyy user. 'hadoop fs -put dracut.log /user/wyy/dracut/'
4. Run this script by typing 'pig -f dracut.q'
*/

/* actual query */
A = LOAD 'hdfs://localhost/user/wyy/dracut/dracut.log' USING PigStorage(' '); 
B = FOREACH A GENERATE REGEX_EXTRACT($8, '^.*/([A-Za-z0-9.-]*)\'$', 1) AS file; 
C = GROUP B BY file;
D = FOREACH C GENERATE FLATTEN($0) as filegroup, COUNT(B) as count;
E = ORDER D BY count,filegroup DESC;
STORE E INTO 'hdfs://localhost/user/wyy/dracut_summary.log' USING PigStorage('|');
