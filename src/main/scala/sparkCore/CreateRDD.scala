package sparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 弹性
    存储的弹性 内存与磁盘的自动切换
    容错的弹性 数据丢失可以自动恢复
    计算的弹性 计算出错重试机制
    分片的弹性 可根据需要重新分片
 * 分布式 数据存储在大数据集群不同节点上
 * 数据集 RDD封装了计算逻辑并不保存数据
 * 数据抽象 RDD是一个抽象类 需要子类具体实现
 * 不可变 RDD封装了计算逻辑 是不可以改变的 想要改变 只能产生新的RDD 在新的RDD里面封装计算逻辑
 * 可分区 并行计算
 *
 * SparkCore编程中RDD使用各种算子相互转化类似于JAVA IO的包装 InputStream->BufferedInputStream->BufferedReader
 * 可以类比成HadoopRDD->MapPartitionsRDD->MapPartitionsRDD->ShuffledRDD 通过一系列的包装 将运算逻辑包装了进去 是一种装饰者模式
 */
object CreateRDD {
  def main(args: Array[String]): Unit = {
    /*
    local 使用一个工作线程在本地运行Spark(即根本没有并行性).
    local[K] 使用K个工作线程在本地运行Spark(理想情况下 将其设置为计算机上的核心数)
    local[*] 使用与计算机上的逻辑核心一样多的工作线程在本地运行Spark
     */
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CreateRDD")
    val sc: SparkContext = new SparkContext(conf)

    val seq1: Seq[Int] = Seq(1, 2, 3, 4)

    // 默认分区数为当前配置spark.default.parallelism 若没有配置则为当前可用核心数
    val rdd1: RDD[Int] = sc.makeRDD(seq1, 4)
    println(rdd1)
    rdd1.foreach(println)

    // 将数据保存成为分区文件
    rdd1.saveAsTextFile("output")

    println("------------------")
    val rdd2: RDD[Int] = sc.parallelize(seq1)
    rdd2.collect().foreach(println)

    println("------------------")
    val path: String = CreateRDD.getClass.getResource("/wc.txt").getPath
    // textFile 可以读取文件也可以读取路径 每次读取一行
    // 默认分区数为核心数和2的最小值
    // 具体算法使用的是hadoop的1.1倍分区数算法
    val rdd3: RDD[String] = sc.textFile(path, 2)
    rdd3.collect().foreach(println)

    println("------------------")
    // (文件名, 文件内容)
    val rdd4: RDD[(String, String)] = sc.wholeTextFiles(path)
    rdd4.foreach(println)

    sc.stop()
  }
}
