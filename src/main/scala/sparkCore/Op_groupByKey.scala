package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Op_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 5), ("b", 6)))
    // 对key-value键值对的数据按照key值进行分组 形成一个对偶元组
    // 元组中第一个元素为key 第二个元素为value的集合
    val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
    rdd2.collect().foreach(println)

    // groupBy不一定按照key值进行分组 需要指定分组规则
    // 并且形成的对偶元组中的第二个元素 是原本k-v键值对的集合
    val rdd3: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(_._1)
    rdd3.collect().foreach(println)



    /*
    spark中shuffle操作必须落盘处理 不能在内存中等待数据(shuffle过后分区内的数据必须齐全才能进行下一步转换)
    在内存中等待数据会导致内存溢出 所以shuffle的效率非常低

    reduceByKey和groupByKey的区别
    1 从shuffle的角度 reduceByKey和groupByKey都存在shuffle操作
    但是reduceByKey可以在shuffle前对分区内相同key的数据进行预聚合（combine）
    这样会减少落盘的数据量 而groupByKey只是进行分组 不存在数据量减少的问题 reduceByKey性能比较高
    2 从功能的角度 reduceByKey其实包含分组和聚合的功能 groupByKey只能分组 不能聚合
    所以在分组聚合的场合下推荐使用reduceByKey
    如果仅仅是分组而不需要聚合 那么还是只能使用groupByKey
     */









    sc.stop()
  }
}
