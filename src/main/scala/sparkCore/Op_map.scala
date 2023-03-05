package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Op_map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val f1 = (x: Int) => {
      x * 2
    }

    rdd1.map(f1).collect().foreach(println)
    //    rdd1.map((data: Int) => data * 2).collect().foreach(println)
    //    rdd1.map(data => data * 2).collect().foreach(println)
    //    rdd1.map(_ * 2).collect().foreach(println)

    // rdd的计算过程 查看结果发现rdd的运算是分区内的每个值走完全部算子后 下一个值才开始计算
    // 分区内的值是有序计算的
    // 不同分区的值计算是无序的
    val rdd2: RDD[Int] = rdd1.map(
      num => {
        println("-----" + num)
        num
      }
    )
    val rdd3: RDD[Int] = rdd2.map(num => {
      println("#####" + num)
      num
    })

    rdd3.collect()


    sc.stop()
  }
}
