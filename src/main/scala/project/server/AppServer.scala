package project.server

import org.apache.spark.rdd.RDD
import project.dao.AppDao

class AppServer {

  private val dao: AppDao = new AppDao

  def analysis: Array[(String, Int)] = {

    val rdd: RDD[String] = dao.getData(dao.getClass.getResource("/wc.txt").getPath)
    val tuples: Array[(String, Int)] = rdd.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).collect()
    tuples
  }
}
