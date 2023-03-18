package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object SparkWithMySQL {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // 方式1 通用的load方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://bigdata01:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load().show

    // 方式2 通用的load方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://bigdata01:3306/spark-sql?user=root&password=123456",
        "dbtable" -> "user",
        "driver" -> "com.mysql.jdbc.Driver"))
      .load()
      .show

    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://bigdata01:3306/spark-sql", "user", props)
    df.show


    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://bigdata01:3306/spark-sql")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }
}
