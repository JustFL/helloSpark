package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkCustomUDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCustomUDAF")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json(SparkCustomUDAF.getClass.getResource("/user.json").getPath)
    df.createTempView("user")

    session.udf.register("ageAvg", new MyAvg)
    session.sql("select ageAvg(age) from user").show()

    session.close()
  }

  // 弱类型 根据索引来操作
  class MyAvg extends UserDefinedAggregateFunction {

    // 输入数据的结构
    override def inputSchema: StructType = {
      StructType(Seq(StructField("age", LongType)))
    }

    // 缓冲区的数据结构
    override def bufferSchema: StructType = {
      StructType(Seq(StructField("total", LongType), StructField("count", LongType)))
    }

    // 输出数据的结构
    override def dataType: DataType = LongType

    // 函数稳定性 传入相同参数结果是否相同
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 参数1为位置 参数2为初始化值
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, input.getLong(0) + buffer.getLong(0))
      buffer.update(1, 1 + buffer.getLong(1))
    }

    // 分布式计算中的多个缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
