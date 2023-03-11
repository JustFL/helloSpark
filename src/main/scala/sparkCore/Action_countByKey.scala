package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action_countByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 统计rdd中所有值出现的次数
    val map1: collection.Map[Int, Long] = rdd1.countByValue()
    println(map1)

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)))
    val map2: collection.Map[String, Long] = rdd2.countByKey()
    println(map2)


    sc.stop()
  }
}
