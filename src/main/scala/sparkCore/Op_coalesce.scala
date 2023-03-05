package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object Op_coalesce {
  def main(args: Array[String]): Unit = {
    /*
      根据数据量缩减分区 当spark程序中 存在过多的小任务的时候 可以通过coalesce方法 收缩合并分区
      减少分区的个数 减小任务调度成本
     */

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // coalesce默认方法不会对分区数据进行打乱重新组合 只是将原本的分区数据合并从而减少分区 可能会导致数据不均衡 出现数据倾斜
    rdd1.coalesce(2).saveAsTextFile("output")

    // coalesce第二个参数指定是否shuffle 对数据进行重新分配
    rdd1.coalesce(2, true).saveAsTextFile("output1")

    sc.stop()
  }
}
