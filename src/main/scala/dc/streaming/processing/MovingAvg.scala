package dc.streaming.processing

import java.util.UUID

import dc.streaming.common.KafkaAndAbnormalDBWriter.formatKafkaData
import dc.streaming.common.KafkaDataWithRuleReader
import it.unimi.dsi.fastutil.longs.{Long2DoubleLinkedOpenHashMap, Long2DoubleRBTreeMap, Long2ObjectOpenHashMap}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by shirukai on 2019-02-12 10:22
  *
  * 移动平均
  *
  * 参数：
  * microservice-dc-1:9092
  * dc-time-data-mock
  * microservice-dc-1:9092
  * dc-out-test-1
  * 1000
  * http://microservice-dc-1:8088/rulemgr/api/ruleInstanceWithBindByCode?code=clean_moving_average
  * "10 minutes"
  * 36000000
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/moving-avg/
  */
object MovingAvg extends Logging {
  def main(args: Array[String]): Unit = {

    if (args.length < 8) {
      System.err.println("Usage: CumulativeAmountWithExceptionTime <input-bootstrap-servers> <input-topics> <output-bootstrap-servers> <output-topics> " +
        "<max-offsets-per-trigger> <rule-service-api> <watermark-delay-threshold> <wait-restart-time> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(inputBootstrapServers, inputTopics, outputBootstrapServers, outputTopics, maxOffsetsPerTrigger, ruleServiceAPI, watermarkDelayThreshold,
    waitRestartTime, _*) = args

    val checkpointLocation =
      if (args.length > 8) args(8) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      //      .master("local[2]")
      //.config("spark.sql.shuffle.partitions", 10)
      .config("dfs.client.use.datanode.hostname", "true")
      //.config("spark.sql.codegen.wholeStage", value = false)
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

    def process(source: DataFrame): DataFrame = {
      source
        .as[Device]
        .withWatermark("eventTime", watermarkDelayThreshold)
        .groupByKey(_.pointId)
        .flatMapGroupsWithState(
          outputMode = OutputMode.Append(),
          timeoutConf = GroupStateTimeout.NoTimeout)(func = calculate)
        .toDF()
    }

    def writeBatch(dataFrame: DataFrame, batchId: Long): Unit = {
      //      dataFrame.show(false)
      formatKafkaData(dataFrame.withColumn("t", $"eventTime")).write
        .format("kafka")
        .option("kafka.bootstrap.servers", outputBootstrapServers)
        .option("topic", outputTopics)
        .save()
    }

    while (true) {
      val stream = KafkaDataWithRuleReader.joinRules(source, ruleServiceAPI, ruleOptions)
      val movingValueQuery = process(stream.filter($"id".isNotNull))
        .writeStream
        .foreachBatch(writeBatch _)
        .option("checkpointLocation", checkpointLocation + "moving/")
        .start()

      val otherQuery = stream.filter($"id".isNull)
        .writeStream
        .foreachBatch(writeBatch _)
        .option("checkpointLocation", checkpointLocation + "other")
        .start()

      Thread.sleep(waitRestartTime.toInt)
      while (movingValueQuery.status.isTriggerActive) {}
      movingValueQuery.stop()
      while (otherQuery.status.isTriggerActive) {}
      otherQuery.stop()
      movingValueQuery.awaitTermination()
      otherQuery.awaitTermination()
      logWarning("====================query 正在重启=====================")
    }
  }

  case class Device(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double, s: String, eventTime: Long)

  class MovingAvgState(windowDuration: Long = 60) extends Serializable {

    // history state
    private val historyState = new Long2DoubleRBTreeMap()

    // latest time of state
    private var latestTime = 0L

    def clear(expired: Long): Unit = {
      historyState.headMap(expired).clear()
    }

    def add(t: Long, v: Double, id: String): Long2DoubleLinkedOpenHashMap = {
      val values = new Long2DoubleLinkedOpenHashMap()

      // calculate interval
      val interval = t - latestTime

      val normalCondition = interval == t || 0 < interval && interval <= windowDuration
      if (normalCondition) {
        values.put(t, AvgBox.calculate(t, v))
      }

      if (windowDuration < interval) {
        AvgBox.clear()
        values.put(t, AvgBox.calculate(t, v))
      }

      if (interval < 0) {
        AvgBox.clear()
        import scala.collection.JavaConverters._
        val history = historyState.subMap(t - windowDuration, t)
        if (!history.isEmpty) {
          history.asScala.foreach(x => {
            AvgBox.calculate(x._1, x._2, updateTime = false)
          })
        }
        values.put(t, AvgBox.calculate(t, v))
        val influence = historyState.subMap(t, t + windowDuration)
        if (!history.isEmpty) {
          influence.asScala.foreach(x => {
            values.put(x._1, AvgBox.calculate(x._1, x._2, updateTime = false))
          })
        }

      }
      historyState.put(t, v)
      values
    }

    object AvgBox {
      val box: mutable.ArrayBuffer[(Long, Double)] = mutable.ArrayBuffer[(Long, Double)]()
      var sum = 0.0
      var size = 0

      def calculate(t: Long, v: Double, updateTime: Boolean = true): Double = {
        box.dropWhile(x => {
          val isDrop = x._1 < t - windowDuration
          if (isDrop) {
            sum -= x._2
            size -= 1
          }
          isDrop
        })

        sum += v
        size += 1
        box.append((t, v))
        if (updateTime) latestTime = t
        if (sum > 0) sum / size
        else 0.0
      }

      def clear(): Unit = {
        sum = 0.0
        size = 0
        box.clear()
      }
    }

  }

  type State = MovingAvgState
  implicit val State: Encoder[MovingAvgState] = org.apache.spark.sql.Encoders.kryo[MovingAvgState]

  def calculate(id: String, devices: Iterator[Device], historyState: GroupState[State]): Iterator[Device] = {
    import scala.collection.JavaConversions._
    // Handle new data
    val deviceList = devices.to[ListBuffer].sortBy(_.eventTime)

    val updateDevices = new Long2ObjectOpenHashMap[Device]()
    if (deviceList.isEmpty) return updateDevices.values().iterator()

    // Filter out expired data in historical stat from spark
    val avgState: State = historyState.getOption.getOrElse(new MovingAvgState())
    val filterKey = historyState.getCurrentWatermarkMs() / 1000
    avgState.clear(filterKey)

    deviceList.foreach(device => {
      val values = avgState.add(device.eventTime, device.v, id)
      values.foreach(x => {
        updateDevices.put(x._1, Device(device.gatewayId, device.namespace, device.pointId, device.id, device.uri, x._2, device.s, x._1))
      })
    })
    historyState.update(avgState)
    updateDevices.valuesIterator
  }
}
