package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object Op_repartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // coalesce可以扩大分区数量 但是必须使用shuffle 因为如果不进行shuffle则表示数据不能打乱 则新加的分区必定是空分区 所以没有意义
    rdd1.coalesce(3, true).saveAsTextFile("output")

    // 扩大分区推荐使用repartition 该操作内部其实执行的是coalesce操作 参数shuffle的默认值为true
    // 其实无论增加分区个数或者缩小分区个数 repartition操作都可以完成 因为无论如何都会经shuffle过程
    rdd1.repartition(3).saveAsTextFile("output1")

    sc.stop()
  }
}
