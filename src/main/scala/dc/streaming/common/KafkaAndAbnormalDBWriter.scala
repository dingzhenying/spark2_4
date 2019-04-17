package dc.streaming.common

import dc.streaming.common.CalculateStateManager.TimePeriodWriteMode
import dc.streaming.processing.AmplitudeWithExceptionTimeNoDelay.ExceptionWriteMode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
  * Created by shirukai on 2019-02-28 15:07
  * kafka数据写入、异常数据库数据写入
  */
trait KafkaAndAbnormalDBWriter extends Serializable with Logging {

  import KafkaAndAbnormalDBWriter._

  private val options = new scala.collection.mutable.HashMap[String, String]

  def handleBatch(row: Row, abnormalDBDao: AbnormalDBDao)

  def option(key: String, value: String): KafkaAndAbnormalDBWriter = {
    this.options += (key -> value)
    this
  }

  def setBootstrapServices(bootstrapServers: String): KafkaAndAbnormalDBWriter = {
    this.options("bootstrapServers") = bootstrapServers
    this
  }

  def setTopic(topic: String): KafkaAndAbnormalDBWriter = {
    this.options("topic") = topic
    this
  }

  def setDBUsername(username: String): KafkaAndAbnormalDBWriter = {
    this.options("username") = username
    this
  }

  def setDBPassword(password: String): KafkaAndAbnormalDBWriter = {
    this.options("password") = password
    this
  }

  def setDBUrl(url: String): KafkaAndAbnormalDBWriter = {
    this.options("url") = url
    this
  }

  def save(df: DataFrame, batch: Long): Unit = {
    df.persist()
    // 正常值
    val normal = df
      .filter(s"exceptionWriteMode=${ExceptionWriteMode.NO_WRITE} " +
        s"AND timePeriodWriteMode=${TimePeriodWriteMode.NO_WRITE}")
    // 写入kafka
    log.debug("The normal data after processing is being written to kafka.")
    formatKafkaData(normal).write
      .format("kafka")
      .option("kafka.bootstrap.servers", options("bootstrapServers"))
      .option("topic", options("topic"))
      .save()

    // 异常数据写入数据库
    df.filter(s"exceptionWriteMode!=${ExceptionWriteMode.NO_WRITE} " +
      s"OR timePeriodWriteMode !=${TimePeriodWriteMode.NO_WRITE}").foreachPartition(records => {
      if (records.nonEmpty) {
        log.debug("The processed exception data is being written to timescaledb.")
        val abnormalDBDao = AbnormalDBDao(options("username"), options("password"), options("url"))
        records.foreach(row => handleBatch(row, abnormalDBDao))
        abnormalDBDao.commitAndClose()
      }
    })
    df.unpersist()
  }
}

object KafkaAndAbnormalDBWriter {
  def formatKafkaData(df: DataFrame): DataFrame = {
    df.select(col("pointId").as("key"),
      to_json(struct("gatewayId", "namespace", "pointId", "v", "s", "t")).as("value"))
  }
}