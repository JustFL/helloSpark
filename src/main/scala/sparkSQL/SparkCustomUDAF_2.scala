package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object SparkCustomUDAF_2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCustomUDAF")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json(SparkCustomUDAF.getClass.getResource("/user.json").getPath)
    df.createTempView("user")

    session.udf.register("ageAvg", functions.udaf(new Avg))
    session.sql("select ageAvg(age) from user").show()

    session.close()
  }

  case class Buff(var total: Long, var count: Long)

  /*
  3个泛型：
  IN 输入的数据类型
  BUFF 缓冲区的数据类型
  OUT 输出的数据类型
   */
  class Avg extends Aggregator[Long, Buff, Long] {
    // 缓冲区数据初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区
    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    // 合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区编码
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出数据编码
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
