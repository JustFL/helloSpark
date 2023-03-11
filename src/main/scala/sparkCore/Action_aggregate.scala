package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action_aggregate {
  def main(args: Array[String]): Unit = {
    // 不同于转换算子aggregateByKey aggregate是一个行动算子

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // aggregateByKey：初始值只会参与分区内的计算
    // aggregate : 初始值不仅参数分区内计算 而且参与分区间计算
    println(rdd1.aggregate(5)(_ + _, _ + _))

    sc.stop()
  }
}
