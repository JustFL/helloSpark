package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Random

object DIYReceiver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(4))

    // 接受自定义采集器的数据
    ssc.receiverStream(new DIYReceiver()).print()

    ssc.start()
    ssc.awaitTermination()
  }

  // 继承Receiver 定义接受数据的类型 定义存储级别
  class DIYReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    private var flag: Boolean = true

    // 启动采集器
    override def onStart(): Unit = {
      // 单独线程
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            val msg: String = new Random().nextInt(10).toString
            store(msg)

            Thread.sleep(500)
          }
        }
      }).start()
    }

    // 关闭采集器
    override def onStop(): Unit = {
      flag = false
    }
  }
}
