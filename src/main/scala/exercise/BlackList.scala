package exercise

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BlackList {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackList")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaDStream(ssc, Seq("exercise_topic"))
    val dataDSteam: DStream[String] = kafkaDStream.map(x => x.value())

    dataDSteam.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
