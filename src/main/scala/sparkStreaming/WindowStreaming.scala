package sparkStreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowStreaming {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf: SparkConf = new SparkConf().setAppName("WindowStreaming").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    val context = new StreamingContext(sparkContext, Seconds(2))

    val lineStream: ReceiverInputDStream[String] = context.socketTextStream("bigdata01", 9999)
    val resStream: DStream[(String, Int)] = lineStream.flatMap(_.split(" "))
      .map((_, 1))
      // 窗口大小和滑动步长必须是采集周期的整数倍
      .reduceByKeyAndWindow(((x: Int, y: Int) => x + y), Seconds(4), Seconds(2))
    resStream.print()

    context.start()
    context.awaitTermination()
  }
}
