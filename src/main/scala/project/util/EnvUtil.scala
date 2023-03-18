package project.util

import org.apache.spark.SparkContext

object EnvUtil {

  private val scLocal: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]

  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def get: SparkContext = {
    scLocal.get()
  }

  def clear: Unit = {
    scLocal.remove()
  }

}
