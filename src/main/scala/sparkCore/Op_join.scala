package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Op_join {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("e", 5)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 15), ("d", 2), ("a", 3), ("e", 5)))

    // 两个kv类型的RDD 相同key的value会连接在一起 形成元组
    // 如果两个RDD中的key没有匹配上 那么数据不会出现在结果中
    // 如果两个RDD中的key有多个相同的 那么会依次连接在一起 形成笛卡尔乘积 造成数据几何暴涨
    rdd1.join(rdd2).collect().foreach(println)

    println("-------------")
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)

    println("-------------")
    rdd1.rightOuterJoin(rdd2).collect().foreach(println)

    println("-------------")
    // cogroup每个RDD中先进行聚合 返回一个Iterable 然后与另外一个RDD对相同的key进行连接
    rdd1.cogroup(rdd2).collect().foreach(println)

    sc.stop()
  }
}
