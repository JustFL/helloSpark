package project.dao

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import project.util.EnvUtil

class AppDao {

  def getData(path: String): RDD[String] = {
    val sc: SparkContext = EnvUtil.get
    sc.textFile(path)
  }
}
