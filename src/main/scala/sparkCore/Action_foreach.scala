package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action_foreach {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // collect已经将数据收集到driver端 并且返回一个集合 所以此处foreach是集合的方法
    rdd1.collect().foreach(println)
    println("-----------")
    // rdd的foreach方法是在Executor端进行打印
    rdd1.foreach(println)

    /*
    RDD的方法和Scala集合的方法不同
    集合对象的方法都是在同一个节点的内存中执行的
    RDD的方法可以将计算逻辑发送到分布式节点（Executor）执行
    为了区分所以称为算子
    RDD调用的方法都是在Executor端执行的 RDD调用方法的外部的语句都是在Driver端执行的
     */


    sc.stop()
  }
}
