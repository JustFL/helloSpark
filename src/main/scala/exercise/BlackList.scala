package exercise

import com.alibaba.fastjson.JSONObject
import exercise.MySQLUtil.{modifyData, queryData}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.Connection
import scala.collection.mutable.ListBuffer

object BlackList {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackList")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaDStream(ssc, Seq("exercise_topic"))
    val objDStream: DStream[UserAdCount] = kafkaDStream.map(x => {
      val data: Array[String] = x.value().split(" ")
      UserAdCount(data(0), data(1), data(2), data(3).toInt, data(4).toInt)
    })

    val clickCountInOneCycle: DStream[((String, Int, Int), Int)] = objDStream.transform {
      rdd => {
        // 周期性查询userid是否在黑名单中
        val connection: Connection = MySQLUtil.getConnection
        val rs: ListBuffer[JSONObject] = queryData(connection, "select * from black_list")
        MySQLUtil.closeConnection(connection)
        val blackList: ListBuffer[Integer] = rs.map(jsonObj => {
          jsonObj.getInteger("userid")
        })
        // 过滤掉在黑名单中的数据
        val whiteData: RDD[UserAdCount] = rdd.filter(data => !blackList.contains(data.userid))

        // 不在黑名单中的数据 统计每个采集周期中 每天 每人 每个广告的点击次数
        whiteData.map(data => ((data.date, data.userid, data.adid), 1)).reduceByKey(_ + _)
      }
    }

    def addBlackList(connection: Connection, userid: Int) = {
      val sql: String =
        """
          | insert into black_list(userid) values (?)
          | on duplicate key
          | update userid = ?
          |""".stripMargin
      modifyData(connection, sql, Array(userid, userid))
    }


    clickCountInOneCycle.foreachRDD(rdd => {
      /*
      优化 foreach会对每条数据进行一次操作
      下面数据库操作每次都会申请连接 这样针对每条数据都建立多次数据库连接的操作太浪费资源
      但是如果在rdd.foreach外创建连接 则是在Driver端创建连接
      需要传递到Executor端 涉及闭包操作 但是连接对象不能序列化
      所以优化方式是使用foreachPartition 对RDD的每个parition创建一次连接
       */
      rdd.foreachPartition {
        iter => {
          val connection: Connection = MySQLUtil.getConnection
          iter.foreach {
            case ((date, userid, adid), count) => {
              println(s"$date $userid $adid $count")
              if (count > 30) {
                // 单个周期内点击次数超过阈值 则加入黑名单
                addBlackList(connection, userid)
              } else {
                // 周期内没超过阈值 则更新数据库点击量
                val sql: String =
                  """
                    | select * from user_ad_count where dt = ? and userid = ? and adid = ?
                    |""".stripMargin
                val resList: ListBuffer[JSONObject] = queryData(connection, sql, Array(date, userid, adid))

                // 如果已经存在 则更新
                if (resList.length > 0) {
                  val sql: String =
                    """
                      | update user_ad_count
                      | set count = count + ?
                      | where dt = ? and userid = ? and adid = ?
                      |""".stripMargin
                  modifyData(connection, sql, Array(count, date, userid, adid))

                  // 更新完成后 查看点击数是否超过阈值
                  val sql1: String =
                    """
                      | select * from user_ad_count where dt = ? and userid = ? and adid = ? and count > 30
                      |""".stripMargin
                  val resList1: ListBuffer[JSONObject] = queryData(connection, sql1, Array(date, userid, adid))
                  // 如果有结果 表示已经超过阈值 加入黑名单
                  if (resList1.length > 0) {
                    addBlackList(connection, userid)
                  }
                } else {
                  // 如果不存在 则新增
                  val sql: String =
                    """
                      | insert into user_ad_count(dt, userid, adid, count) values (?,?,?,?)
                      |""".stripMargin
                  modifyData(connection, sql, Array(date, userid, adid, count))
                }
              }
            }
          }
          MySQLUtil.closeConnection(connection)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }


  case class UserAdCount(date: String, area: String, city: String, userid: Int, adid: Int)
}
