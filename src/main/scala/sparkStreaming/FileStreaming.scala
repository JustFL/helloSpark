package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreaming {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))

    // 文件必须要在程序启动后修改一次 添加内容 或者修改文件名字 这样程序才能识别（文件的修改日期一定要晚于程序启动时间）
    ssc.textFileStream("input").flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).print()

    // 启动采集器
    ssc.start()

    // 等待采集器的结束
    ssc.awaitTermination()
  }
}
