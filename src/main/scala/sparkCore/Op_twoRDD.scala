package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object Op_twoRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

    // 交集
    println(rdd1.intersection(rdd2).collect().mkString(","))

    // 并集
    println(rdd1.union(rdd2).collect().mkString(","))

    // 差集
    println(rdd1.subtract(rdd2).collect().mkString(","))

    // 拉链 要求两个RDD分区数量一致并且每个分区中的元素数量一致
    println(rdd1.zip(rdd2).collect().mkString(","))

    sc.stop()
  }
}
