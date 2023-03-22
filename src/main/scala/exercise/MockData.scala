package exercise

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

object MockData {

  def getProps: Properties = {
    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata01:9092,bigdata02:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def main(args: Array[String]): Unit = {

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](getProps)

    while (true) {

      // 发送一批数据到kafka
      val datas: ListBuffer[String] = Mock()

      for (elem <- datas) {
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("exercise_topic", elem)
        producer.send(record)
      }

      // 休息
      Thread.sleep(1000)
    }

  }

  def Mock(): ListBuffer[String] = {

    /*
    格式 ：timestamp area city userid adid
     */

    val datas: ListBuffer[String] = new ListBuffer[String]

    for (elem <- 1 to new Random().nextInt(30)) {

      var data: String = ""

      val timestamp: Long = System.currentTimeMillis()

      val areas: Seq[String] = Seq("山西", "山东", "河南", "河北")
      val area: String = areas(new Random().nextInt(areas.length))

      val cities: Seq[String] = Seq("阳泉", "济南", "郑州", "石家庄", "天津")
      val city: String = cities(new Random().nextInt(cities.length))

      val userid = new Random().nextInt(5)
      val adid = new Random().nextInt(8)

      data = timestamp + " " + area + " " + city + " " + userid + " " + adid
      datas.append(data)
    }
    datas
  }
}
