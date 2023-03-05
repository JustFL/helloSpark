package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_flatMap {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))

    rdd1.flatMap(list => list).collect().foreach(println)

    // 对不规则数据进行扁平化
    val rdd2: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    rdd2.flatMap(item => {
      item match {
        case list: List[_] => list
        case a => List(a)
      }
    }).collect().foreach(println)


    sc.stop()
  }
}
