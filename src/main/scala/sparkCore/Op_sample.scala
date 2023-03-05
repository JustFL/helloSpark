package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    // 第一个参数：抽取的数据是否放回 false 不放回
    // 第二个参数：抽取的几率 范围在[0,1]之间 0：全不取 1：全取
    // 第三个参数：随机数种子 种子确定后每次抽取的数据也随之确定 不传递此参数会默认传递当前时间戳
    val sampleRDD: RDD[Int] = rdd1.sample(false, 0.4, 1)
    sampleRDD.collect().foreach(println)

    sc.stop()
  }

}
