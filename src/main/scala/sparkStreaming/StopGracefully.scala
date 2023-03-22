package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StopGracefully {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    ssc.start()
    ssc.awaitTermination()
  }
}
