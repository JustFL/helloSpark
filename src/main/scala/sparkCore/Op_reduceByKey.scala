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

    // reduceByKey具有预聚合功能 在shuffle之前会先在分区内进行聚合 shuffle之后相同key的进入同一个分区 再次进行聚合
    // 总结起来就是两步聚合 分区内聚合和分区间聚合 并且是聚合规则相同的两两聚合

    sc.stop()
  }
}
