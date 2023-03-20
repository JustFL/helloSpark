package sparkStreaming

import io.netty.channel.nio.AbstractNioChannel
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Kafka数据源（面试、开发重点）
 * ReceiverAPI 需要一个专门的Executor去接收数据 然后发送给其他的Executor做计算
 * 存在的问题 接收数据的Executor和计算的Executor速度会有所不同
 * 特别在接收数据的Executor速度大于计算的Executor速度会导致计算数据的节点内存溢出
 * 早期版本中提供此方式 当前版本不适用
 *
 * DirectAPI 是由计算的Executor来主动消费Kafka的数据 速度由自身控制
 */
object DirectAPI {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Direct")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bigdata01:9092,bigdata02:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "gid_first",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")

    // 定义Kafka参数
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Seq("first"), kafkaParams))

    // 读取Kafka数据创建DStream
    val valueDS: DStream[String] = kafkaDS.map(record => record.value())

    valueDS.print()

    // kafka-consumer-groups.sh --describe --bootstrap-server bigdata01:9092 --group gid_first 查看当前消费者组的消费进度

    ssc.start()
    ssc.awaitTermination()
  }
}
