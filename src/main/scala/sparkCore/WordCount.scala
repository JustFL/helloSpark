package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)
    //sc.textFile(WordCount.getClass.getResource("/wc.txt").getPath).flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).foreach(println)
    sc.textFile(args(0)).flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).foreach(println)
    sc.stop()
  }
}

