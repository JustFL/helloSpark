package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object Op_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 5), ("b", 6)))

    // 相同key的数据进行value值的聚合
    // 聚合操作和scala中的reduce一样都是两两聚合
    rdd1.reduceByKey(_ + _).collect().foreach(println)

    sc.stop()
  }
}
