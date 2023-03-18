package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * DataFrame与RDD的主要区别在于
 * 前者带有schema元信息 即DataFrame所表示的二维表数据集的每一列都带有名称和类型 这使得Spark SQL得以洞察更多的结构信息
 * 从而对藏于DataFrame背后的数据源以及作用于DataFrame之上的变换进行了针对性的优化 最终达到大幅提升运行时效率的目标
 *
 *
 * DataFrame其实就是DataSet的一个特例
 * type DataFrame = Dataset[Row]
 */
object DFDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName(DFDemo.getClass.getSimpleName)
    conf.setMaster("local[*]")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // RDD=>DataFrame=>DataSet转换需要引入隐式转换规则 否则无法转换
    // session不是包名是上下文环境对象名
    import session.implicits._

    // 读取json 必须一行是一个json串
    val df: DataFrame = session.read.json(this.getClass.getResource("/user.json").getPath)

    // DSL语法
    df.select("id", "name", "age").show(1)
    df.select($"age" + 1).show()
    df.select('age + 1).show()

    // SQL语法
    df.createTempView("user")
    session.sql("select id, name from user").show()

    // 补充global表的内容 使用global后 不论哪个session都可以访问 但是需要指定默认存储的库global_temp
    df.createGlobalTempView("user1")
    session.sql("select * from global_temp.user1").show()

    println("---RDD=>DataFrame=>DataSet---")

    val rdd1: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20)))
    val df1: DataFrame = rdd1.toDF("id", "name", "age")
    df1.show()
    val ds1: Dataset[emp] = df1.as[emp]
    ds1.show()

    println("---DataSet=>DataFrame=>RDD---")

    val df2: DataFrame = ds1.toDF()
    df2.show()
    // 返回的RDD类型为Row 里面提供的getXXX方法可以获取字段值 类似jdbc处理结果集 但是索引从0开始
    val rdd2: RDD[Row] = df2.rdd
    rdd2.foreach(row => println(row.getInt(0) + "---" + row.getString(1) + "---" + row.getInt(2)))

    println("---RDD=>DataSet---")

    val ds2: Dataset[emp] = rdd1.map {
      case (id, name, age) => emp(id, name, age)
    }.toDS()
    ds2.show()


    session.close()
  }

  case class emp(id: Int, name: String, age: Int)
}
