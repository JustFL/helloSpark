package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * cache 将数据临时存储在内存中进行数据重用
 *       会在血缘关系中添加新的依赖
 * persist 可以将数据临时存储在磁盘文件中进行数据重用
 *         涉及到磁盘IO 性能较低 数据安全
 *         如果作业执行完毕 临时保存的磁盘文件会丢失
 * checkpoint 将数据长久的保存在磁盘文件中进行数据重用
 *            任务执行完毕 磁盘文件也不会丢失 执行过程中会切断血缘关系 重新建立新的血缘关系
 *            checkpoint等同于改变数据源
 */
object CheckPoint {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CheckPoint")
    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("cp-dir")

    val list: List[String] = List("Hello Scala", "Hello Spark")

    val rdd1: RDD[String] = sc.makeRDD(list)

    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))

    val rdd3: RDD[(String, Int)] = rdd2.map(data => {
      println("map is executing")
      (data, 1)
    })

    /*
    所谓的检查点其实就是通过将RDD中间结果写入磁盘
    checkpoint会导致任务重新再次执行一遍 所以一般会先进行cache 第二次执行从cache用取数 减小开销
     */
    rdd3.cache()
    rdd3.checkpoint()

    println(rdd3.toDebugString)
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)
    rdd4.collect().foreach(println)

    println(rdd3.toDebugString)

    println("---------------------------")

    val rdd5: RDD[(String, Iterable[Int])] = rdd3.groupByKey()
    rdd5.collect().foreach(println)


    sc.stop()
  }
}
