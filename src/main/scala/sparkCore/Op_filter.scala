package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 将数据根据指定的规则进行筛选过滤 符合规则的数据保留 不符合规则的数据丢弃
    // 当数据进行筛选过滤后 分区不变 但是分区内的数据可能不均衡 生产环境下可能会出现数据倾斜
    rdd1.filter(x => x % 2 == 0).collect().foreach(println)


    sc.stop()
  }
}
