package dc.streaming.common

import dc.streaming.common.CalculateStateManager.{ExceptionWriteMode, TimePeriodWriteMode}
import dc.streaming.util.DruidUtils
import org.apache.http.client.fluent.Request
import org.apache.spark.internal.Logging

/**
  * Created by shirukai on 2019-02-27 16:48
  * 异常库：数据访问层
  */
class AbnormalDBDao(user: String, password: String, url: String, commitSize: Int = 1000)
  extends Serializable with Logging {

  import AbnormalDBDao._

  private val options = Map[String, String](
    "jdbcUrl" -> url,
    "username" -> user,
    "password" -> password,
    "driverClassName" -> "org.postgresql.Driver"
  )
  private lazy val connection = DruidUtils.createDataSource(options).get.getConnection

  private lazy val stmt = {
    // 禁止自动提交
    connection.setAutoCommit(false)
    connection.createStatement()
  }

  private var offset = 0

  def addBatch(t: Long, uri: String, v: Double, upperbound: String, lowerbound: String, exceptionType: String,
               exceptionStartTime: Long, exceptionEndTime: Long, exceptionWriteMode: Int, timePeriodWriteMode: Int): Unit = {
    // 判断异常写入模式
    if (exceptionWriteMode == ExceptionWriteMode.INSERT) insertDiscTable(t, uri, v, exceptionType, upperbound, lowerbound)
    else if (exceptionWriteMode == ExceptionWriteMode.DELETE) deleteDiscTable(uri, t)
    handleTimeRangeTable(uri, t, exceptionType, exceptionStartTime, exceptionEndTime, timePeriodWriteMode)
    if (offset == commitSize) commit()
    offset += 1
  }

  def addBatch(t: Long, uri: String, v: Double, previousValue: Double, exceptionType: String, exceptionStartTime: Long,
               exceptionEndTime: Long, exceptionWriteMode: Int, timePeriodWriteMode: Int): Unit = {
    // 判断异常写入模式
    if (exceptionWriteMode == ExceptionWriteMode.INSERT) insertDiscTable(t, uri, v, exceptionType, previousValue)
    else if (exceptionWriteMode == ExceptionWriteMode.DELETE) deleteDiscTable(uri, t)
    handleTimeRangeTable(uri, t, exceptionType, exceptionStartTime, exceptionEndTime, timePeriodWriteMode)
    if (offset == commitSize) commit()
  }

  def addBatch(t: Long, uri: String, v: Double, exceptionType: String, exceptionStartTime: Long, exceptionEndTime: Long,
               exceptionWriteMode: Int, timePeriodWriteMode: Int): Unit = {
    // 判断异常写入模式
    if (exceptionWriteMode == ExceptionWriteMode.INSERT) insertDiscTable(t, uri, v, exceptionType)
    else if (exceptionWriteMode == ExceptionWriteMode.DELETE) deleteDiscTable(uri, t)
    handleTimeRangeTable(uri, t, exceptionType, exceptionStartTime, exceptionEndTime, timePeriodWriteMode)
    if (offset == commitSize) commit()
  }

  //for switch value
  def addSwitchBatch(t: Long, uri: String, v: Double, switchDefault: String, exceptionType: String, exceptionStartTime: Long, exceptionEndTime: Long,
                     exceptionWriteMode: Int, timePeriodWriteMode: Int): Unit = {
    // 判断异常写入模式
    if (exceptionWriteMode == ExceptionWriteMode.INSERT) insertDiscTable(t, uri, v, exceptionType, switchDefault)
    else if (exceptionWriteMode == ExceptionWriteMode.DELETE) deleteDiscTable(uri, t)
    handleTimeRangeTable(uri, t, exceptionType, exceptionStartTime, exceptionEndTime, timePeriodWriteMode)
    if (offset == commitSize) commit()
  }


  def commit(): Unit = {
    try {
      stmt.executeBatch()
    } catch {
      case e: Exception =>
        println(e)
        connection.rollback()
        throw new RuntimeException(s"commit error.${e.getMessage}")
    } finally connection.commit()
    offset = 0
  }

  def commitAndClose(): Unit = {
    commit()
    close()
  }

  def close(): Unit = {
    if (stmt != null) {
      stmt.close()
    }
    if (connection != null) {
      connection.close()
    }

  }


  def handleTimeRangeTable(uri: String, t: Long, exceptionType: String, exceptionStartTime: Long, exceptionEndTime: Long,
                           timePeriodWriteMode: Int): Unit = {
    if (timePeriodWriteMode == TimePeriodWriteMode.INSERT)
      insertTimeRangeTable(uri, t, exceptionType, exceptionStartTime, exceptionEndTime)
    else if (timePeriodWriteMode == TimePeriodWriteMode.UPDATE)
      updateTimeRangeTable(uri, t, exceptionType, exceptionStartTime, exceptionEndTime)
    else if (timePeriodWriteMode == TimePeriodWriteMode.DELETE) deleteTimeRangeTable(uri, t)

  }


  def insertDiscTable(t: Long, uri: String, v: Double, exceptionType: String, upperbound: String, lowerbound: String): Unit = {
    stmt.addBatch(s"INSERT INTO $DISCRIMINATION_TABLE_NAME (node_uri,time,value,abnormal_type,upper_bound,lower_bound) " +
      s"VALUES ('$uri','$t','$v','$exceptionType','$upperbound','$lowerbound') on conflict(node_uri,time,abnormal_type) do nothing")
  }

  def insertDiscTable(t: Long, uri: String, v: Double, exceptionType: String, previousValue: Double): Unit = {
    stmt.addBatch(s"INSERT INTO $DISCRIMINATION_TABLE_NAME (node_uri,time,value,abnormal_type,previous_value) " +
      s"VALUES ('$uri','$t','$v','$exceptionType','$previousValue') on conflict(node_uri,time,abnormal_type) do nothing")
  }

  def insertDiscTable(t: Long, uri: String, v: Double, exceptionType: String): Unit = {
    stmt.addBatch(s"INSERT INTO $DISCRIMINATION_TABLE_NAME (node_uri,time,value,abnormal_type) " +
      s"VALUES ('$uri','$t','$v','$exceptionType') on conflict(node_uri,time,abnormal_type) do nothing")
  }

  //for switch default value
  def insertDiscTable(t: Long, uri: String, v: Double, exceptionType: String, switchDefault: String): Unit = {
    stmt.addBatch(s"INSERT INTO $DISCRIMINATION_TABLE_NAME (node_uri,time,value,abnormal_type,switch_default) " +
      s"VALUES ('$uri','$t','$v','$exceptionType','$switchDefault') on conflict(node_uri,time,abnormal_type) do nothing")
  }

  def insertTimeRangeTable(uri: String, t: Long, exceptionType: String, exceptionStartTime: Long, exceptionEndTime: Long): Unit = {
    stmt.addBatch(s"INSERT INTO $TIME_RANGE_TABLE_NAME (node_uri,last_abnormal_time,abnormal_type,abnormal_start_time,abnormal_end_time) " +
      s"VALUES ('$uri','$t','$exceptionType','$exceptionStartTime','$exceptionEndTime') on conflict(node_uri,last_abnormal_time,abnormal_type) do nothing")
  }


  def updateTimeRangeTable(uri: String, t: Long, exceptionType: String, exceptionStartTime: Long, exceptionEndTime: Long): Unit = {
    stmt.addBatch(s"UPDATE $TIME_RANGE_TABLE_NAME SET abnormal_start_time='$exceptionStartTime',abnormal_end_time='$exceptionEndTime'" +
      s"WHERE node_uri='$uri' AND last_abnormal_time='$t'")
  }

  def deleteDiscTable(uri: String, t: Long): Unit = {
    stmt.addBatch(s"DELETE FROM $DISCRIMINATION_TABLE_NAME WHERE node_uri='$uri' AND time='$t'")
  }

  def deleteTimeRangeTable(uri: String, t: Long): Unit = {
    stmt.addBatch(s"DELETE FROM $TIME_RANGE_TABLE_NAME WHERE node_uri='$uri' AND last_abnormal_time='$t'")
  }


}

object AbnormalDBDao {
  val DISCRIMINATION_TABLE_NAME = "discrimination_abnormal_data"
  val TIME_RANGE_TABLE_NAME = "abnormal_time_range"

  def apply(user: String, password: String, url: String, commitSize: Int = 1000): AbnormalDBDao = new AbnormalDBDao(user, password, url, commitSize)

  def main(args: Array[String]): Unit = {
    val api = "http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_moving_average"
    val res = Request.Get(api).execute().returnResponse()
    println(res)

  }
}
