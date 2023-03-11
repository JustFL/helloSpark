package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action_reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 3, 4), 2)
    // 行动算子会提交一个job 发起任务的执行 并且不同于转换算子 返回的不再是RDD
    // reduce 两两聚合RDD中的所有元素 先聚合分区内数据 再聚合分区间数据
    println(rdd1.reduce(_ + _))



    sc.stop()
  }
}
