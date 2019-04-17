package dc.streaming.processing

import java.util
import java.util.UUID

import dc.streaming.common.CalculateStateManager.{State, StateInfo, TimePeriodWriteMode}
import dc.streaming.common.{AbnormalDBDao, CalculateTrait, KafkaAndAbnormalDBWriter, KafkaDataWithRuleReader}
import dc.streaming.processing.LimitDiscriminateWithExceptionTime.logWarning
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by cuijunheng on 2019-02-21 11:51
  * 幅度变化过大判别，通过FlatMapGroupsWithState函数实现
  *
  * 参数：
  * microservice-dc-1:9092
  * dc-time-data-mock
  * microservice-dc-1:9092
  * dc-out-test-1
  * 1000
  * http://microservice-dc-1:8088/rulemgr/api/ruleInstanceWithBindByCode?code=dis_excess_ampli_change
  * "10 minutes"
  * 36000000
  * postgres
  * postgres
  * jdbc:postgresql://timescaledb-dc-1:5432/dataclean_dev
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/amplitude-with-exception-time/
  *
  */
object AmplitudeWithExceptionTimeWithDelay extends Logging {
  def main(args: Array[String]): Unit = {

    if (args.length < 11) {
      System.err.println("Usage: AmplitudeWithExceptionTimeWithDelay <input-bootstrap-servers> <input-topics> <output-bootstrap-servers> <output-topics> " +
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
      //      .config("spark.sql.shuffle.partitions", 10)
      .config("dfs.client.use.datanode.hostname", "true")
     // .master("local[1]")
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
      "dc.cleaning.rule.column.amp" -> "$.0.value"
    )


    while (true) {
      val stream = KafkaDataWithRuleReader.joinRules(source, ruleServiceAPI, ruleOptions)

      val amplitudeQuery = process(stream.filter($"id".isNotNull))
        .writeStream
        .foreachBatch(write _)
        .option("checkpointLocation", checkpointLocation + "amplitude/")
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
      while (amplitudeQuery.status.isTriggerActive){}
      amplitudeQuery.stop()
      while (otherQuery.status.isTriggerActive){}
      amplitudeQuery.stop()
      otherQuery.stop()
      amplitudeQuery.awaitTermination()
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


    def write(value: DataFrame, batchId: Long): Unit = {
      new KafkaAndAbnormalDBWriter {
        override def handleBatch(row: Row, abnormalDBDao: AbnormalDBDao): Unit = {
          abnormalDBDao.addBatch(row.getAs("t"), row.getAs("uri"),
            row.getAs("v"), row.getAs("lastValue"), exceptionType,
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


  case class Device(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double, s: String, t: Long, amp: String)

  case class DeviceWithException(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double,
                                 lastValue: Double, s: String, t: Long, exceptionStartTime: Long, exceptionEndTime: Long,
                                 exceptionWriteMode: Int, timePeriodWriteMode: Int)
  //case class DeviceWithException(gatewayId: String, namespace: String, pointId: String, id: String, v: Double, s: String, t: Long, exceptionStart: Long, exceptionEnd: Long)

  class CalculateService(watermarkTime: Int = 60 * 10) extends CalculateTrait[Device, DeviceWithException] {

    //override lazy val watermark: Int = watermarkTime

    // 判断当前值是否异常
    override def currentExceptionDiscriminator(lastState: Option[util.Map.Entry[Long, StateInfo]], device: Device): Boolean = {
      println(device)
      if (lastState.isDefined) {
        println(lastState.get)
        val amp = device.amp
        val cur_value = device.v
        val prev_value = lastState.get.getValue.value

        !(((cur_value - prev_value) / prev_value <= amp.toDouble) && ((cur_value - prev_value) / prev_value >= amp.toDouble * (-1)))
      } else {
        false
      }

    }

    // 判断下一个值是否异常
    override def nextExceptionDiscriminator(device: Device, nextState: Option[util.Map.Entry[Long, StateInfo]]): Boolean = {

      if (nextState.isDefined) {
        val amp = device.amp
        val cur_value = device.v
        val next_value = nextState.get.getValue.value
        !(((next_value - cur_value) / cur_value <= amp.toDouble) && ((next_value - cur_value) / cur_value >= amp.toDouble * (-1)))
      } else {
        false
      }

    }

    // 数据预处理
    override def handleDevices(devices: Iterator[Device]): ListBuffer[Device] = {
      devices.to[ListBuffer].sortBy(_.t)
    }

    // 当前状态处理
    override def deviceState(device: Device): (Long, StateInfo) = (device.t, StateInfo(device.v, 0, 0))

    // 返回结果处理
    override def handleDeviceWithException(device: Device, timestamp: Long, state: StateInfo, exceptionWriteMode: Int, timePeriodWriteMode: Int): DeviceWithException = {
      DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, device.uri, state.value, state.lastValue, device.s, timestamp, state.exceptionStartTime, state.exceptionEndTime, exceptionWriteMode, timePeriodWriteMode)

    }

    override def handleLastDevice(lastDevice: DeviceWithException): DeviceWithException = {
      lastDevice.copy(timePeriodWriteMode = TimePeriodWriteMode.INSERT)
    }
  }

}

