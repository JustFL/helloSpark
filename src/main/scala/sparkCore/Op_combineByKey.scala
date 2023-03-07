package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Op_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 3), ("a", 7), ("a", 4)), 2)

    // 和aggregateByKey相似
    // 区别在于第一个参数不是初始值 而是对分区内相同key的第一个值进行的转化操作
    // 第二和第三个参数与aggregateByKey相同
    rdd1.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    ).collect().foreach(println)

    sc.stop()
  }
}
