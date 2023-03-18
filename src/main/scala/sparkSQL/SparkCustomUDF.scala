package sparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 选中想被try/catch包围的语句 同时按下ctrl+alt+t
 * 补全等号左边语句 Ctrl+Alt+V+回车
 * 查看接口的实现类：Ctrl+Alt+B
 * 查看类继承关系图：Ctrl+Alt+U
 * 查看当前类的继承树：Ctrl+H
 * 查看一个类中有什么方法：Alt+7
 *
 */

object SparkCustomUDF {
  def main(args: Array[String]): Unit = {

    val path: String = SparkCustomUDF.getClass.getResource("/").getPath.toString + "student.txt"

    val session: SparkSession = new SparkSession.Builder().appName("SparkUserFunction").master("local").getOrCreate()
    import session.implicits._

    val rdd: RDD[String] = session.sparkContext.textFile(path)
    val arrRDD: RDD[Array[String]] = rdd.map(_.split(","))
    val tupleRDD: RDD[(Int, String, Int)] = arrRDD.map(array => {
      (array(0).toInt, array(1).toString, array(2).toInt)
    })
    val df: DataFrame = tupleRDD.toDF("id", "name", "age")
    df.show()

    /*type DataFrame = Dataset[Row]*/
    df.createTempView("stuView")
    session.udf.register("len", ((x: String) => x.length))
    session.sql("select id, len(name), name from stuView").show()

    val df1: DataFrame = session.sql("select id, len(name), name from stuView")
    val rdd1: RDD[Any] = df1.rdd.map(_.getInt(1))
    rdd1.foreach(println)

    session.stop()
  }
}
