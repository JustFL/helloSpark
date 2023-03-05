package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_groupBy extends App {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
  val sc: SparkContext = new SparkContext(conf)

  val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
  val f1 = (x: Int) => {
    x % 2
  }

  // groupBy 将RDD中所有数据映射为组的key值 相同key的数据划分为一组
  rdd1.groupBy(f1).collect().foreach(println)

  // groupBy 分区数不变 但原本数据打乱重新组合 称为shuffle
  // 极限情况下 数据可能被分在同一个分区中
  // 一个组的数据在一个分区中 但是并不是说一个分区中只有一个组


  val rdd2: RDD[String] = sc.makeRDD(List("hello", "spark", "hive", "scala"))
  // 将rdd中每个元素映射为首字母 即将首字母作为key进行分组
  rdd2.groupBy(str => str.charAt(0)).collect().foreach(println)



  sc.stop()

}
