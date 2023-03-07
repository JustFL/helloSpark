package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_foldByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")
    val sc: SparkContext = new SparkContext(conf)

    // 当分区内计算规则和分区间计算规则相同时 aggregateByKey就可以简化为foldByKey
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 7), ("b", 4)), 2)
    rdd1.foldByKey(0)(_ + _).collect().foreach(println)

    sc.stop()
  }
}
