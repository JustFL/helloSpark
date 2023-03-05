package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_glom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 将同一个分区的数据直接转换为相同类型的内存数组进行处理 分区不变
    val rdd2: RDD[Array[Int]] = rdd1.glom()
    rdd2.collect().foreach(arr => println(arr.mkString(",")))


    // 计算所有分区最大值求和（分区内取最大值 分区间最大值求和）
    val rdd3: RDD[Array[Int]] = rdd1.glom()
    val rdd4: RDD[Int] = rdd3.map(arr => arr.max)
    val arr1: Array[Int] = rdd4.collect()
    println(arr1.sum)


    sc.stop()
  }
}
