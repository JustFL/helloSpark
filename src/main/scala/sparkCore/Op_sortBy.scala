package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Op_sortBy {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(6, 2, 3, 4, 1, 5), 2)
    // 按照传递的规则进行排序 第二个参数可以倒叙
    // 排序后新产生的RDD的分区数与原RDD的分区数一致 中间存在shuffle的过程
    rdd1.sortBy(num => num, false).saveAsTextFile("output")

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 1), ("2", 1)))
    rdd2.sortBy(x => x._1).collect().foreach(println)


    sc.stop()
  }
}
