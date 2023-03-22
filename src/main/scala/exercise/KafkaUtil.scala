package exercise

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaUtil {

  def getKafkaParam: Map[String, Object] = {

    val param: Map[String, Object] = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropUtil.getProperty("BOOTSTRAP_SERVERS_CONFIG"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> PropUtil.getProperty("KEY_DESERIALIZER_CLASS_CONFIG"),
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> PropUtil.getProperty("VALUE_DESERIALIZER_CLASS_CONFIG"))

    param
  }

  def getKafkaDStream(ssc: StreamingContext, topic: Seq[String]): InputDStream[ConsumerRecord[String, String]] = {

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, getKafkaParam))
    dStream
  }
}
