package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadSave {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCustomUDAF")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val path: String = LoadSave.getClass.getResource("/").getPath

    // spark.read.load是加载数据的通用方法 SparkSQL默认读取和保存的文件格式为parquet
    val df1: DataFrame = session.read.load(path + "users.parquet")
    df1.show()


    /*
    如果读取不同格式的数据 可以对不同的数据格式进行设定
    session.read.format()[.option()].load()
    format() 指定加载的数据类型包括"csv"、"jdbc"、"json"、"orc"、"parquet"、"textFile"
     */
    val df2: DataFrame = session.read.format("json").load(path + "user.json")
    df2.show()


    // 直接在文件上进行查询 文件格式.`文件路径`
    session.sql("select * from json.`" + path + "/user.json`").show

    // df.write.save 是保存数据的通用方法 默认格式为parquet
    df2.write.save("output")

    // 指定文件格式
    df2.write.format("json").save("output1")

    // 指定保存模式
    df2.write.format("json").mode("append").save("output1")

    session.close()
  }
}
