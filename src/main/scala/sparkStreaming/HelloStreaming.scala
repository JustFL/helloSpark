package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object HelloStreaming {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))

    /*
    监听网络端口 使用nc -lk port命令发送字符串
    print()在运行流程序的驱动结点上打印DStream中每一批次数据的最开始10个元素 这用于开发和调试
    */
    ssc.socketTextStream("bigdata01", 8888).map(x => (x, 1)).reduceByKey(_ + _).print()

    // 启动采集器
    ssc.start()

    // 等待采集器的结束
    ssc.awaitTermination()
  }
}
