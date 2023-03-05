package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_MapPartitions extends App {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
  val sc: SparkContext = new SparkContext(conf)

  val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

  // 以分区为单位进行转化 一次处理一个分区内的数据 提高效率
  // 但是会将一个分区内的数据进行引用 部分数据执行完转化也不会释放 直到所有数据执行完毕
  // 在数据量大 内存较小的情况下 可能会导致内存溢出
  rdd1.mapPartitions(iter => {
    println("----")
    iter.map(_ * 2)
  }).collect().foreach(println)

  // 获取每个分区内的最大值 注意mapPartitions要求输入一个Iterator 返回一个Iterator
  rdd1.mapPartitions(iter => {
    List(iter.max).toIterator
  }).collect().foreach(println)

  sc.stop()
}
