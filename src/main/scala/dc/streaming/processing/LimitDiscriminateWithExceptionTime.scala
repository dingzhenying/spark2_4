package dc.streaming.processing

import java.util
import java.util.UUID

import dc.streaming.common.{AbnormalDBDao, CalculateTrait, KafkaAndAbnormalDBWriter, KafkaDataWithRuleReader}
import dc.streaming.common.CalculateStateManager.{State, StateInfo, TimePeriodWriteMode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

import scala.collection.mutable.ListBuffer

/**
  * Created by shirukai on 2019-02-21 11:51
  * 累计量判别，通过FlatMapGroupsWithState函数实现
  *
  * 参数：
  * microservice-dc-1:9092
  * dc-time-data-mock
  * microservice-dc-1:9092
  * dc-out-test-2
  * 1000
  * http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=dis_upper_lower_limits
  * "10 minutes"
  * 36000000
  * postgres
  * postgres
  * jdbc:postgresql://timescaledb-dc-1:5432/dataclean_dev
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/limit-discriminate-with-exception-time/
  **/
object LimitDiscriminateWithExceptionTime extends Logging {
  def main(args: Array[String]): Unit = {

    if (args.length < 11) {
      System.err.println("Usage: LimitDiscriminateWithExceptionTime <input-bootstrap-servers> <input-topics> <output-bootstrap-servers> <output-topics> " +
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
      //.master("local[2]")
      .appName(this.getClass.getSimpleName)
      //.config("spark.sql.shuffle.partitions", 10)
      .config("dfs.client.use.datanode.hostname", "true")
      .getOrCreate()

    import spark.implicits._

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
      "dc.cleaning.rule.column.upperbound" -> "$.0.value",
      "dc.cleaning.rule.column.lowerbound" -> "$.1.value"
    )


    while (true) {
      val stream = KafkaDataWithRuleReader.joinRules(source, ruleServiceAPI, ruleOptions)
      val limitQuery = process(stream.filter($"id".isNotNull))
        .writeStream
        .foreachBatch(limitWrite _)
        .option("checkpointLocation", checkpointLocation + "limit/")
        .start()

      val otherQuery = KafkaAndAbnormalDBWriter
        .formatKafkaData(stream.filter($"id".isNull))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", outputBootstrapServers)
        .option("topic", outputTopics)
        .option("checkpointLocation", checkpointLocation + "other/")
        .start()

      Thread.sleep(waitRestartTime.toLong)
      while (limitQuery.status.isTriggerActive) {}
      limitQuery.stop()
      while (otherQuery.status.isTriggerActive) {}
      otherQuery.stop()
      limitQuery.awaitTermination()
      otherQuery.awaitTermination()
      logWarning("====================query 正在重启=====================")
    }

    def process(source: DataFrame): DataFrame = {
      val calculateService = new CalculateService()
      source
        .as[Device]
        .withWatermark("eventTime", watermarkDelayThreshold)
        .groupByKey(_.id)
        .flatMapGroupsWithState(
          outputMode = OutputMode.Append(),
          timeoutConf = GroupStateTimeout.NoTimeout)(func = calculateService.calculate).toDF()
    }
    def limitWrite(limitWrite: DataFrame, batchId: Long): Unit = {
      new KafkaAndAbnormalDBWriter {
        override def handleBatch(row: Row, abnormalDBDao: AbnormalDBDao): Unit = {
          abnormalDBDao.addBatch(row.getAs("t"), row.getAs("uri"),
            row.getAs("v"), row.getAs("upperbound"), row.getAs("lowerbound"),
            exceptionType, row.getAs("exceptionStartTime"), row.getAs("exceptionEndTime"),
            row.getAs("exceptionWriteMode"), row.getAs("timePeriodWriteMode"))
        }
      }.setBootstrapServices(outputBootstrapServers)
        .setTopic(outputTopics)
        .setDBUsername(postgresUser)
        .setDBPassword(postgresPassword)
        .setDBUrl(postgresUrl)
        .save(limitWrite, batchId)
    }

  }


  def limitDiscriminate(value: Double, lower: Double, upper: Double): Boolean = lower > value || value > upper

  case class Device(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double, s: String, t: Long, upperbound: String, lowerbound: String)

  case class DeviceWithLimitException(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double,
                                      upperbound: String, lowerbound: String, s: String, t: Long, exceptionStartTime: Long,
                                      exceptionEndTime: Long, exceptionWriteMode: Int, timePeriodWriteMode: Int)


  class CalculateService extends CalculateTrait[Device, DeviceWithLimitException] {

    // 判断当前值是否异常
    override def currentExceptionDiscriminator(lastState: Option[util.Map.Entry[Long, StateInfo]], device: Device): Boolean = {
      limitDiscriminate(device.v, device.lowerbound.toDouble, device.upperbound.toDouble)
    }

    // 判断下一个值是否异常
    override def nextExceptionDiscriminator(device: Device, nextState: Option[util.Map.Entry[Long, StateInfo]]): Boolean = {
      nextState.isDefined && nextState.get.getValue.exceptionStartTime > 0
    }

    // 数据预处理
    override def handleDevices(devices: Iterator[Device]): ListBuffer[Device] = {
      devices.to[ListBuffer].sortBy(_.t)
    }

    // 当前状态处理
    override def deviceState(device: Device): (Long, StateInfo) = (device.t, StateInfo(device.v))

    // 返回结果处理
    override def handleDeviceWithException(device: Device, timestamp: Long, state: StateInfo, exceptionWriteMode: Int,
                                           timePeriodWriteMode: Int): DeviceWithLimitException = {
      DeviceWithLimitException(device.gatewayId, device.namespace, device.pointId, device.id, device.uri, state.value,
        device.upperbound, device.lowerbound, device.s, timestamp, state.exceptionStartTime, state.exceptionEndTime,
        exceptionWriteMode, timePeriodWriteMode)
    }

    override def handleLastDevice(lastDevice: DeviceWithLimitException): DeviceWithLimitException = {
      lastDevice.copy(timePeriodWriteMode = TimePeriodWriteMode.INSERT)
    }
  }


}

