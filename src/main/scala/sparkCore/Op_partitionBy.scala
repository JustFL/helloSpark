package sparkCore

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Op_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List((1, 99), (2, 99), (3, 99), (4, 99)), 2)
    // Partitioner中定义分区规则 将数据按照指定Partitioner重新进行分区 spark默认的分区器是HashPartitioner 肯定需要shuffle操作
    rdd1.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")


    sc.stop()
  }
}
