package dc.streaming.processing

import java.util
import java.util.UUID

import dc.streaming.common.CalculateStateManager.{StateInfo, TimePeriodWriteMode}
import dc.streaming.common.{AbnormalDBDao, CalculateTrait, KafkaAndAbnormalDBWriter, KafkaDataWithRuleReader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  *
  * 开关量判别
  *
  * 参数：
  * microservice-dc-1:9092
  * dc-time-data-mock
  * microservice-dc-1:9092
  * dc-out-test-1
  * 1000
  * http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=dis_switch
  * "10 minutes"
  * 36000000
  * postgres
  * postgres
  * jdbc:postgresql://timescaledb-dc-1:5432/dataclean_dev
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/switch-value-with-exception-time/
  */
object SwitchValueWithExceptionTime extends Logging {

  def main(args: Array[String]): Unit = {

    if (args.length < 11) {
      System.err.println("Usage: SwitchValueWithExceptionTime <input-bootstrap-servers> <input-topics> <output-bootstrap-servers> <output-topics> " +
        "<max-offsets-per-trigger> <rule-service-api> <watermark-delay-threshold> <wait-restart-time> <postgres-user> " +
        "<postgres-password> <postgres-url> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(inputBootstrapServers, inputTopics, outputBootstrapServers, outputTopics, maxOffsetsPerTrigger, ruleServiceAPI, watermarkDelayThreshold,
    waitRestartTime, postgresUser, postgresPassword, postgresUrl, _*) = args

    val exceptionType = ruleServiceAPI.substring(ruleServiceAPI.indexOf("=") + 1)
    val checkpointLocation =
      if (args.length > 11) args(11) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .config("dfs.client.use.datanode.hostname", "true")
      //.master("local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", inputBootstrapServers)
      .option("subscribe", inputTopics)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      //      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()
    val ruleOptions = Map[String, String](
      "dc.cleaning.rule.column.switch_default" -> "$.0.value"
    )
    while (true) {
      val stream = KafkaDataWithRuleReader.joinRules(source, ruleServiceAPI, ruleOptions)
      val switchQuery = process(stream.filter($"id".isNotNull))
        .writeStream
        .foreachBatch(write _)
        .option("checkpointLocation", checkpointLocation + "switch/")
        .start()

      val otherQuery = KafkaAndAbnormalDBWriter
        .formatKafkaData(stream.filter($"id".isNull))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", outputBootstrapServers)
        .option("topic", outputTopics)
        .option("checkpointLocation", checkpointLocation + "other/")
        .start()

      Thread.sleep(waitRestartTime.toInt)
      while (switchQuery.status.isTriggerActive) {}
      switchQuery.stop()
      while (otherQuery.status.isTriggerActive) {}
      otherQuery.stop()
      switchQuery.awaitTermination()
      otherQuery.awaitTermination()
      logWarning("====================query 正在重启=====================")
    }

    def process(source: DataFrame): DataFrame = {

      val exceptionRangeService = new ExceptionRangeService()
      source
        .as[Switch]
        .withWatermark("eventTime", watermarkDelayThreshold)
        .groupByKey(_.id)
        .flatMapGroupsWithState(
          outputMode = OutputMode.Append(),
          timeoutConf = GroupStateTimeout.NoTimeout)(func = exceptionRangeService.calculate)
        .toDF()

    }


    def write(value: DataFrame, batchId: Long): Unit = {
      new KafkaAndAbnormalDBWriter {
        override def handleBatch(row: Row, abnormalDBDao: AbnormalDBDao): Unit = {
          //println("KafkaAndAbnormalDBWriter:"+row)
          abnormalDBDao.addSwitchBatch(row.getAs("t"), row.getAs("uri"),
            row.getAs("v"), row.getAs("switch_default"), exceptionType,
            row.getAs("exceptionStartTime"), row.getAs("exceptionEndTime"),
            row.getAs("exceptionWriteMode"), row.getAs("timePeriodWriteMode"))
        }
      }.setBootstrapServices(outputBootstrapServers)
        .setTopic(outputTopics)
        .setDBUsername(postgresUser)
        .setDBPassword(postgresPassword)
        .setDBUrl(postgresUrl)
        .save(value, batchId)
    }

  }

  //value、default自动转成int
  def switchCheck(value: Int, default: Int): Boolean = value != default

  //for test
  //def switchCheck(value: Int, default: Int): Boolean = Random.nextBoolean()


  case class Switch(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double, s: String, t: Long, switch_default: String)

  case class SwitchWithExceptionRange(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double,
                                      switch_default: String, s: String, t: Long, exceptionStartTime: Long,
                                      exceptionEndTime: Long, exceptionWriteMode: Int, timePeriodWriteMode: Int)

  class ExceptionRangeService extends CalculateTrait[Switch, SwitchWithExceptionRange] {

    // 判断当前值是否异常
    override def currentExceptionDiscriminator(lastState: Option[util.Map.Entry[Long, StateInfo]], device: Switch): Boolean = {
      val flag = switchCheck(device.v.toInt, device.switch_default.toInt)
      //println(flag + "," + device)
      flag
    }

    // 判断下一个值是否异常
    override def nextExceptionDiscriminator(device: Switch, nextState: Option[util.Map.Entry[Long, StateInfo]]): Boolean = {
      nextState.isDefined && nextState.get.getValue.exceptionStartTime > 0
    }

    // 数据预处理
    override def handleDevices(devices: Iterator[Switch]): ListBuffer[Switch] = {
      devices.to[ListBuffer].sortBy(_.t)
    }

    // 当前状态处理
    override def deviceState(device: Switch): (Long, StateInfo) = (device.t, StateInfo(device.v))

    // 返回结果处理
    override def handleDeviceWithException(device: Switch, timestamp: Long, state: StateInfo, exceptionWriteMode: Int, timePeriodWriteMode: Int): SwitchWithExceptionRange = {
      SwitchWithExceptionRange(device.gatewayId, device.namespace, device.pointId, device.id, device.uri, state.value,
        device.switch_default, device.s, timestamp, state.exceptionStartTime, state.exceptionEndTime, exceptionWriteMode, timePeriodWriteMode)
    }

    override def handleLastDevice(lastDevice: SwitchWithExceptionRange): SwitchWithExceptionRange = {
      lastDevice.copy(timePeriodWriteMode = TimePeriodWriteMode.INSERT)
    }
  }

}
