package sparkStreaming

import exercise.HDFSUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/*
借助外部优雅的关闭流处理程序 所谓优雅指 不是立即关闭 而是把当前数据处理完成后再关闭
 */
object StopGracefully {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Streaming")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val inputDS: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 7777)
    inputDS.print()

    // 启动采集器
    ssc.start()

    // 启动新的线程来控制程序关闭
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          // 在第三方程序中增加关闭状态
          if (HDFSUtil.sparkStreamingStop) {
            // 判断Streaming状态是否运行
            if (ssc.getState() == StreamingContextState.ACTIVE) {
              // 计算节点不在接受新的数据 将现有数据计算完毕 然后关闭
              ssc.stop(true, true)
              // 停止线程
              System.exit(0)
            }
          }
          // 每隔一段时间去检测状态
          println("检测程序正在执行")
          Thread.sleep(5000)
        }
      }
    }).start()

    // awaitTermination会阻塞当前线程
    ssc.awaitTermination()


  }
}
