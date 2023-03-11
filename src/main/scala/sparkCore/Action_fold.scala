package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action_fold {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 当分区内和分区间的计算逻辑相同时 可以使用fold代替aggregate
    // 类似于foldByKey替代aggregateByKey
    println(rdd1.fold(5)(_ + _))

    sc.stop()
  }
}
