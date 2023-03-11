package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action_save {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)


    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)))

    rdd1.saveAsTextFile("output1")
    rdd1.saveAsObjectFile("output2")

    // SequenceFile必须时key-value类型
    rdd1.saveAsSequenceFile("output3")

    sc.stop()
  }
}
