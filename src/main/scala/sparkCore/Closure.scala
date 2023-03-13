package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Closure {
  def main(args: Array[String]): Unit = {
    /*
    重点 难点警告
    从计算的角度
    算子以外的代码都是在Driver端执行
    算子里面的代码都是在Executor端执行
    在scala的函数式编程中 算子内经常会用到算子外的数据
    这样就形成了闭包的效果 如果使用的算子外的数据无法序列化
    就意味着无法传值给Executor端执行 就会发生错误
    所以需要在执行任务计算前 检测闭包内的对象是否可以进行序列化 这个操作我们称之为闭包检测
    序列化可以混入Serializable或者形成样例类
     */

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Op")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val a: A = new A
    rdd1.foreach(
      // 算子内使用算子外的变量 变量必须可序列化或者是样例类的对象
      data => println("A:" + data + a.num)
    )


    val rdd2: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    val search = new Search("hello")
    search.getMatch1(rdd2).collect().foreach(println)
    search.getMatch2(rdd2).collect().foreach(println)


    sc.stop()
  }

  case class A() {
    val num = 30
  }


  /*
   这个例子绕了半天 就说明一个事儿 主构造器传入的参数query就是Search类的属性
   在调用getMatch1时 将isMatch函数传入到了filter算子中 所以在Executor端执行
   isMatch中s.contains(query) query其实是this.query 所以this即Search类必须序列化
   同理getMatch2中使用匿名函数 但是query还是this.query 所以Search类也必须使用序列化
   总结 算子中如果要使用某个类的属性值 则这个类必须序列化
   */
  class Search(query: String) extends Serializable {

    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }
}
