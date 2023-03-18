package project.common

import org.apache.spark.{SparkConf, SparkContext}
import project.util.EnvUtil

trait Environment {

  def createEnvironment(master: String = "local[*]", appName: String = "Application")(op: => Unit): Unit = {

    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc: SparkContext = new SparkContext(conf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear
  }
}
