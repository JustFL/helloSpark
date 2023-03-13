package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDCache {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val list: List[String] = List("Hello Scala", "Hello Spark")

    val rdd1: RDD[String] = sc.makeRDD(list)

    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))

    val rdd3: RDD[(String, Int)] = rdd2.map(data => {
      println("map is executing")
      (data, 1)
    })

    /*
    因为RDD是不存储数据的 所以reduceByKey和groupByKey看似都从rdd3中取数进行计算 实则每次都会从头算起
    将rdd3进行缓存后 则reduceByKey和groupByKey则会从缓存中的数据算起
    cache默认是调用的persist(StorageLevel.MEMORY_ONLY)方法 默认缓存在JVM的堆内存中
    如果希望存储在磁盘中则需使用persist()方法将存储级别传入

    注意：并不是这两个方法被调用时立即缓存 而是触发后面的action算子时 该RDD将会被缓存在计算节点的内存中 并供后面重用
     */

    rdd3.cache()
    println(rdd3.toDebugString)

    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)
    rdd4.collect().foreach(println)

    println(rdd4.toDebugString)

    println("--------------------")

    val rdd5: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    rdd5.collect().foreach(println)




    sc.stop()
  }
}
