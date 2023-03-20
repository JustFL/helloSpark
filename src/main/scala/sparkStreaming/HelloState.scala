package sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object HelloState {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HelloTransform")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("cp-dir")

    val inputDS: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 8888)

    /*
    无状态操作是针对当前采集周期的数据进行处理
    如果需要保留数据的统计结果 实现数据的汇总就需要状态操作
    updateStateByKey 根据key对数据的状态进行更新
    使用有状态操作时 需要设定检查点路径
     */
    inputDS.flatMap(_.split(" ")).map(x => (x, 1)).updateStateByKey(
      // 第一个参数为当前周期中相同key的value数据集合
      // 第二个参数为缓存中相同key的value数据
      (seq: Seq[Int], buff: Option[Int]) => Some(buff.getOrElse(0) + seq.sum)
    ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
