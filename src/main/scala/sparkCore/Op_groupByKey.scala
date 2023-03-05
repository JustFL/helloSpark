package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Op_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 5), ("b", 6)))
    // 对key-value键值对的数据按照key值进行分组 形成一个对偶元组
    // 元组中第一个元素为key 第二个元素为value的集合
    val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
    rdd2.collect().foreach(println)

    // groupBy不一定按照key值进行分组 需要指定分组规则
    // 并且形成的对偶元组中的第二个元素 是原本k-v键值对本身的集合
    val rdd3: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(_._1)
    rdd3.collect().foreach(println)

    sc.stop()
  }
}
