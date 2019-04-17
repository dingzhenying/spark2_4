package dc.streaming.processing

import java.util
import java.util.UUID

import dc.streaming.common.CalculateStateManager.{StateInfo, TimePeriodWriteMode}
import dc.streaming.common._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by shirukai on 2019-02-28 15:07
  * 累计量判别，通过FlatMapGroupsWithState函数实现
  *
  * 参数：
  * microservice-dc-1:9092
  * dc-time-data-mock
  * microservice-dc-1:9092
  * dc-out-test-1
  * 1000
  * http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=dis_cumulative_amount
  * "10 minutes"
  * 36000000
  * postgres
  * postgres
  * jdbc:postgresql://timescaledb-dc-1:5432/dataclean_dev
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/cumulative-amount-with-exception-time/
  **/
object CumulativeAmountWithExceptionTime extends Logging {
  def main(args: Array[String]): Unit = {

    if (args.length < 11) {
      System.err.println("Usage: CumulativeAmountWithExceptionTime <input-bootstrap-servers> <input-topics> <output-bootstrap-servers> <output-topics> " +
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
    val ruleOptions = Map[String, String]()

    while (true) {
      val stream = KafkaDataWithRuleReader.joinRules(source, ruleServiceAPI, ruleOptions)
      val cumulativeQuery = process(stream.filter($"id".isNotNull))
        .writeStream
        //.outputMode("update")
        .foreachBatch(cumulativeWrite _)
        .option("checkpointLocation", checkpointLocation + "cumulative/")
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
      while (cumulativeQuery.status.isTriggerActive) {}
      cumulativeQuery.stop()
      while (otherQuery.status.isTriggerActive) {}
      otherQuery.stop()
      cumulativeQuery.awaitTermination()
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

    def cumulativeWrite(cumulativeValue: DataFrame, batchId: Long): Unit = {
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
        .save(cumulativeValue, batchId)

    }
  }


  case class Device(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double, s: String, t: Long)

  case class DeviceWithException(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double,
                                 lastValue: Double, s: String, t: Long, exceptionStartTime: Long, exceptionEndTime: Long,
                                 exceptionWriteMode: Int, timePeriodWriteMode: Int)

  class CalculateService extends CalculateTrait[Device, DeviceWithException] {
    /**
      * 当批数据预处理
      *
      * @param devices Iterator[A]
      * @return ListBuffer[A]
      */
    override def handleDevices(devices: Iterator[Device]): ListBuffer[Device] = {
      devices.to[ListBuffer].sortBy(_.t)
    }

    /**
      * 判别当前数据是否异常
      *
      * @param lastState 上一条数据的状态
      * @param device    当前数据
      * @return boolean
      */
    override def currentExceptionDiscriminator(lastState: Option[util.Map.Entry[Long, CalculateStateManager.StateInfo]],
                                               device: Device): Boolean = {
      lastState.isDefined && (device.v - lastState.get.getValue.value < 0)
    }

    /**
      * 判别下一条数据是否异常
      *
      * @param device    当前数据
      * @param nextState 下一条数据
      * @return boolean
      */
    override def nextExceptionDiscriminator(device: Device,
                                            nextState: Option[util.Map.Entry[Long, CalculateStateManager.StateInfo]]): Boolean = {
      nextState.isDefined && (nextState.get.getValue.value - device.v < 0)
    }

    /**
      * 状态数据
      *
      * @param device 当前数据
      * @return stateInfo
      */
    override def deviceState(device: Device): (Long, CalculateStateManager.StateInfo) = (device.t, StateInfo(device.v))

    /**
      * 异常数据处理
      *
      * @param device              当前数据
      * @param timestamp           时间戳
      * @param state               状态
      * @param exceptionWriteMode  异常写出模式
      * @param timePeriodWriteMode 异常时间段写出模式
      * @return
      */
    override def handleDeviceWithException(device: Device, timestamp: Long, state: StateInfo, exceptionWriteMode: Int,
                                           timePeriodWriteMode: Int): DeviceWithException = {
      DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, device.uri, state.value, state.lastValue,
        device.s, timestamp, state.exceptionStartTime, state.exceptionEndTime, exceptionWriteMode, timePeriodWriteMode)
    }

    override def handleLastDevice(lastDevice: DeviceWithException): DeviceWithException = {
      lastDevice.copy(timePeriodWriteMode = TimePeriodWriteMode.INSERT)
    }
  }


}
