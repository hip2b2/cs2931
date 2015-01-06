import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object DracutApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Dracut Application")
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://localhost/user/wyy/dracut/dracut.log")
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://localhost:8020/user/wyy/dracut_scala.output")
  }
}
