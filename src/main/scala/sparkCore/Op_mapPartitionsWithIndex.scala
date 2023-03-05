package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 获取rdd第二个分区内的数据
    rdd1.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) iter else Nil.iterator
    }).collect().foreach(println)

    // 显示每个值所在的分区
    rdd1.mapPartitionsWithIndex((index, iter) => {
      iter.map(value => (index, value))
    }).collect().foreach(println)

    sc.stop()
  }
}
