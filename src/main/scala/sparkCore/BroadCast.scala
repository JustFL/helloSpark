package sparkCore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 当Driver通过闭包向Executor传递变量时 有多少个Task就会传递多少次变量
 * 如果数据量过大 Executor节点较少 多个Task会启动在同一个Executor上 每个Task都会保存一份变量 对内存有较大的浪费
 * 广播变量可以将变量广播到Executor上 Executor上的多个Task可以共用这个变量 节省了内存空间
 */
object BroadCast {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val data = 100

    val bcInt: Broadcast[Int] = sc.broadcast(data)

    rdd1.map {
      case (k, v) => {
        (k, v + bcInt.value)
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
