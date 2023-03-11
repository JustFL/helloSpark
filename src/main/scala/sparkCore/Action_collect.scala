package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action_collect {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(4, 2, 3, 3, 1), 2)
    // collect 会将RDD中的数据按照分区顺序数据采集到Driver端的内存中 形成数组
    println(rdd1.collect().mkString(","))

    // count
    println(rdd1.count())

    // first
    println(rdd1.first())

    //
    println(rdd1.take(3).mkString(","))

    //
    println(rdd1.takeOrdered(3).mkString(","))

    sc.stop()
  }
}
