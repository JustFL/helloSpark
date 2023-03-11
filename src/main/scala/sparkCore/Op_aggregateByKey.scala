package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Op_aggregateByKey {
  def main(args: Array[String]): Unit = {
    // 不同于reduceByKey分区内和分区间的聚合规则一致
    // aggregateByKey可以在分区内和分区间聚合时 指定不一样的规则

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")
    val sc: SparkContext = new SparkContext(conf)

    /*
    aggregateByKey 算子是函数柯里化 存在两个参数列表
    第一个参数列表中的参数表示初始值
    第二个参数列表中含有两个参数
      第一个参数表示分区内的计算规则
      第二个参数表示分区间的计算规则
     */


    // 分区内取最大值 分区间相加
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 7), ("a", 4)), 2)
    rdd1.aggregateByKey(5)((x, y) => math.max(x, y), (x, y) => x + y).collect().foreach(println)


    println("------------")

    // 计算平均值
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 3), ("a", 7), ("a", 4)), 2)
    val rdd3: RDD[(String, (Int, Int))] = rdd2.aggregateByKey((0, 0))(
      // 分区内计算 initialValue表示自定义的初始值（0，0） value表示rdd中value值
      (initialValue, value) => (initialValue._1 + 1, initialValue._2 + value),
      // 分区间计算 （count，amount）count表示出现次数 amount表示value的总和
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )

    rdd3.mapValues {
      case (count, amount) => amount / count
    }.collect().foreach(println)


    sc.stop()
  }
}
