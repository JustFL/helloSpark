package sparkSQL

import org.apache.spark.sql.SparkSession

object SparkWithHive {
  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME", "root")
    // 需要hive-site文件 并且需要enableHiveSupport开启
    val session: SparkSession = SparkSession.builder().appName("HiveUseSparkSession").master("local").enableHiveSupport().getOrCreate()
    session.sql("show databases").show()
    session.stop()
  }
}
