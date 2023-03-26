package sparkStreaming

import exercise.HDFSUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HelloForeachRDD {
  def main(args: Array[String]): Unit = {
    /*
    foreachRDD(func)
    这是最通用的输出操作 即将函数func用于产生于stream的每一个RDD
    其中参数传入的函数func应该实现将每一个RDD中数据推送到外部系统
    如将RDD存入文件或者通过网络将其写入数据库
    它用来对DStream中的RDD运行任意计算 这和transform()有些类似
    都可以让我们访问任意RDD 在foreachRDD()中 可以重用我们在Spark中实现的所有行动操作
    比如 常见的用例之一是把数据写到诸如MySQL的外部数据库中
     */

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    /*
    注意
    1.连接不能写在driver层面（序列化）
    2.如果写在foreach 则每个RDD中的每一条数据都创建 得不偿失
    3.增加 foreachPartition 在分区创建（获取）
     */
    val wcDStream: DStream[(String, Int)] = ssc.socketTextStream("bigdata01", 7777).map(x => (x, 1)).reduceByKey(_ + _)

    // 这里想实现将文件存储到hdfs
    wcDStream.foreachRDD(
      rdd => {
        rdd.foreach {
          case (word, count) => {
            println(word + "," + count)
            HDFSUtil.writeToHdfs(word + "," + count)
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
