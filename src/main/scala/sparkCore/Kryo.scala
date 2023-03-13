package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Kryo {
  def main(args: Array[String]): Unit = {

    /*
    简单数据类型 数组和字符串类型 已经在Spark内部使用Kryo来序列化
    注意 即使使用Kryo序列化 也要继承Serializable接口
     */
    val conf: SparkConf = new SparkConf().setAppName("Kryo").setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
      .registerKryoClasses(Array(classOf[K]))


    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val k: K = new K
    rdd1.foreach(
      data => {
        println(data + k.num)
      }
    )

  }

  class K() extends Serializable {
    val num = 1;
  }
}
