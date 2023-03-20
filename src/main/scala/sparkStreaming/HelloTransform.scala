package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HelloTransform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HelloTransform")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val inputDS: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 8888)

    /*
    transform适用场景
    1.DStream功能不完善 获取底层RDD进行操作
    2.需要代码周期性执行
     */

    // Code: Driver端
    val transformDS: DStream[String] = inputDS.transform(rdd => {
      // Code: Driver端(周期性执行)
      rdd.map(data => {
        // Code: Executor端
        data
      })
    })

    // Code: Driver端
    val mapDS: DStream[String] = transformDS.map(
      data => {
        // Code: Executor端
        data
      }
    )

    mapDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
