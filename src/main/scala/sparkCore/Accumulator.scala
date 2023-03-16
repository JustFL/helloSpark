package sparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable

/**
 * 分布式共享只写变量
 * 累加器用来把Executor端变量信息聚合到Driver端
 * 在Driver程序中定义的变量 在Executor端的每个Task都会得到这个变量的一份新的副本
 * 每个task更新这些副本的值后 传回Driver端进行merge
 */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // Driver中的变量
    var sum = 0
    rdd1.foreach(x => {
      // Executor中进行了计算赋值
      sum += x
    })

    // Executor中变量无法回传给Driver 所以Driver中值仍然为0
    println("sum: " + sum)

    println("-----------------------")

    // Spark提供了简单数据类型的累加器
    // 创建并注册一个long类型的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    rdd1.foreach(x => sumAcc.add(x))
    println(sumAcc.value)


    println("-----------------------")
    val sumAcc1: LongAccumulator = sc.longAccumulator("sum1")
    val rdd2: RDD[Int] = rdd1.map(x => {
      sumAcc1.add(x)
      x
    })
    // 如果没有行动算子 累加器的操作不会触发
    // 一般情况下在行动算子中使用累加器
    println(sumAcc1.value)

    println("-----------------------")
    val wcAcc: WcAccumulator = new WcAccumulator
    sc.register(wcAcc, "wcAcc")

    // 使用累加器实现wordcount
    val rdd3: RDD[String] = sc.makeRDD(List("Tom", "Jerry", "Tom", "Hello"), 2)
    rdd3.foreach(x => wcAcc.add(x))
    println(wcAcc.value)

    sc.stop()
  }

  class WcAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

    private val wcMap: mutable.Map[String, Int] = mutable.Map[String, Int]()

    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new WcAccumulator
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(v: String): Unit = {
      val value: Int = wcMap.getOrElse(v, 0) + 1
      wcMap.update(v, value)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      other.value.foreach {
        case (k, v) => {
          val value: Int = wcMap.getOrElse(k, 0) + v
          wcMap.update(k, value)
        }
      }
    }

    override def value: mutable.Map[String, Int] = {
      wcMap
    }
  }
}
