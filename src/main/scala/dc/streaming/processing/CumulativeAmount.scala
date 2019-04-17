package dc.streaming.processing

import java.util.UUID

import dc.streaming.common.KafkaDataWithRuleReader
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author : shirukai
  * @date : 2019-02-18 09:06
  *       累计量判别，通过FlatMapGroupsWithState函数实现
  *       192.168.66.194:9092 subscribe dc-ts-data http://192.168.66.194:8088/rulemgr/ruleInstanceWithBindByCode?code=shxxpb hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/cumulative-amount/
  **/
object CumulativeAmount {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics>[<checkpoint-location>]")
      System.exit(1)
    }

    val Array(bootstrapServers, subscribeType, topics, ruleServiceAPI, _*) = args
    val checkpointLocation =
      if (args.length > 4) args(4) else "/tmp/temporary-" + UUID.randomUUID.toString


    // 等待重启时间1小时
    val waitRestartTime = 1000 * 60 * 60 * 1

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 实例化Reader
    val reader = new KafkaDataWithRuleReader(spark)
      // kafka servers
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("maxOffsetsPerTrigger", "1000")
      // topic
      .option(subscribeType, topics)
      // 规则服务提供的api,参数为规则模板code
      .option("dc.cleaning.rule.service.api", ruleServiceAPI)

    while (true) {
      val source = reader.load()

      val query = process(source.filter($"id".isNotNull)).writeStream.outputMode("update")
        .foreachBatch(writeBatch _)
        .option("checkpointLocation", checkpointLocation)
        .start()

      Thread.sleep(waitRestartTime)
      query.stop()
      query.awaitTermination(1000 * 60 * 5)
    }

    def process(source: DataFrame): DataFrame = {
      source.as[Device].groupByKey(_.id).flatMapGroupsWithState(
        outputMode = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.NoTimeout)(func = calculate).repartition($"id").select("*")
    }

    def writeBatch(dataFrame: DataFrame, batchId: Long): Unit = {
      dataFrame.persist()
      dataFrame.show(false)
      //      val exceptionValues = dfCache.filter($"status".equalTo(false))
      //      val otherValues = dfCache.filter($"status".isNull or $"status".equalTo(true))


      dataFrame.unpersist()
    }

    def exceptionWrite(exceptionValues: DataFrame, batchId: Long): Unit = {

      // TODO 异常数据写入异常库
      exceptionValues.show(false)
    }

    def otherWrite(otherValues: DataFrame, batchId: Long): Unit = {

      // TODO 其他数据写入Kafka
      otherValues.show(false)

    }


  }

  type State = mutable.Map[Long, Double]

  def calculate(id: String, devices: Iterator[Device], state: GroupState[State]): Iterator[DeviceWithRange] = {
    // Get the historical state from spark
    val oldState: State = state.getOption.getOrElse(mutable.Map())

    val updateDeviceWithRanges = ListBuffer[DeviceWithRange]()
    // 对新数据按照时间戳排序
    val deviceList = devices.to[ListBuffer].sortBy(_.t)
    deviceList.foreach(device => {
      val currentTimestamp = device.t
      // 根据t从历史状态中取比t大的第一个值和比t小的第一个值
      val upper = oldState.find(_._1 > currentTimestamp)
      val lower = oldState.find(_._1 < currentTimestamp)


      // 如果upper有值，输出upper
      if (upper.isDefined) {
        updateDeviceWithRanges.append(DeviceWithRange(device.gatewayId, device.namespace, device.pointId, device.id, device.v,
          device.s, device.t, upper.get._2 - device.v))
      }
      // 如果lower有值，输出当前
      if (lower.isDefined) {
        updateDeviceWithRanges.append(DeviceWithRange(device.gatewayId, device.namespace, device.pointId, device.id, device.v,
          device.s, device.t, device.v -  lower.get._2))
      }
      // 将当前值更新到历史状态
      oldState(device.t) = device.v
    })

    // 状态更新到Spark
    state.update(oldState)

    updateDeviceWithRanges.toIterator
  }

  case class Device(gatewayId: String, namespace: String, pointId: String, id: String, v: Double, s: String, t: Long)

  case class DeviceWithRange(gatewayId: String, namespace: String, pointId: String, id: String, v: Double, s: String, t: Long, range: Double)

}
