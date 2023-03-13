package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Op_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List((1, 99), (2, 99), (3, 99), (4, 99)), 2)
    // Partitioner中定义分区规则 将数据按照指定Partitioner重新进行分区 spark默认的分区器是HashPartitioner 肯定需要shuffle操作
    rdd1.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("nba", 1), ("cba", 1), ("cuba", 1), ("cctv", 1), ("wto", 1)))
    rdd2.partitionBy(new C).saveAsTextFile("output1")

    sc.stop()
  }

  // 自定义分区器
  class C extends Partitioner {
    override def numPartitions: Int = 3

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case "cuba" => 2
        case _ => 2
      }
    }
  }
}
