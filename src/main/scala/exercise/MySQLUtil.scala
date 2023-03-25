package exercise

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.alibaba.fastjson.JSONObject

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}
import java.text.SimpleDateFormat
import java.util.{Date}
import scala.collection.mutable.ListBuffer
import javax.sql.DataSource

object MySQLUtil {

  // 初始化连接池
  var dataSource: DataSource = init()

  // 初始化连接池方法
  def init(): DataSource = {
    DruidDataSourceFactory.createDataSource(PropUtil.getProperties())
  }

  // 获取 MySQL 连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  def closeConnection(connection: Connection): Unit = {
    connection.close()
  }

  def queryData(connection: Connection, sql: String, array: Array[Any] = Array.empty): ListBuffer[JSONObject] = {

    val statement: PreparedStatement = connection.prepareStatement(sql)

    if (array.length > 0) {
      for (index <- 0 until array.length) {
        statement.setObject(index + 1, array(index))
      }
    }

    val rs: ResultSet = statement.executeQuery()

    val md: ResultSetMetaData = rs.getMetaData
    val objs: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()

    while (rs.next()) {
      val obj: JSONObject = new JSONObject()
      for (i <- 1 to md.getColumnCount) {
        val colName: String = md.getColumnLabel(i)
        obj.put(colName, rs.getObject(i))
      }
      objs.append(obj)
    }

    // 关闭资源
    rs.close()
    statement.close()

    objs
  }

  def modifyData(connection: Connection, sql: String, values: Array[Any]) = {
    val ps: PreparedStatement = connection.prepareStatement(sql)
    for (index <- 1 to values.length) {
      ps.setObject(index, values(index - 1))
    }
    ps.executeUpdate()
  }
}
