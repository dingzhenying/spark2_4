package dc.streaming.processing

import java.util
import java.util.UUID

import dc.streaming.common.KafkaDataWithRuleReader
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author : cuijunheng
  * @date : 2019-02-18 09:06
  *       幅度变化过大判别，通过FlatMapGroupsWithState函数实现
  *       192.168.66.194:9092 subscribe dc-data http://192.168.66.194:8088/rulemgr/ruleInstanceWithBindByCode?code=dis_cumulative_amount hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/cumulative-amount-with-exception-time/
  ***/
object CumulativeAmountWithExceptionTimeNoDelay {

  object ExceptionWriteMode {
    val INSERT: Int = 1
    val UPDATE: Int = 2
    val DELETE: Int = -1
    val NO_WRITE: Int = 0
  }

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
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    import spark.implicits._

    // 实例化Reader
    val reader = new KafkaDataWithRuleReader(spark)
      // kafka servers
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("maxOffsetsPerTrigger", "1000")
      // topic
      .option(subscribeType, topics)
      // 规则服务提供的api,参数为规则模板code
      .option("dc.cleaning.rule.service.api", ruleServiceAPI)
      //.option("dc.cleaning.rule.column.amp", "$.value")

    while (true) {
      val source = reader.load()

      val cumulativeQuery = process(source.filter($"id".isNotNull))
        .writeStream
        .outputMode("update")
        .trigger(Trigger.ProcessingTime(60*1000))
        .foreachBatch(cumulativeWrite _)
        .option("checkpointLocation", checkpointLocation + "cumulative/")
        .start()

//      val otherQuery = source.filter($"id".isNull)
//        .writeStream
//        .outputMode("update")
//        .foreachBatch(otherWrite _)
//        .option("checkpointLocation", checkpointLocation + "other/")
//        .start()

      Thread.sleep(waitRestartTime)
      cumulativeQuery.stop()
//      otherQuery.stop()
      cumulativeQuery.awaitTermination()
//      otherQuery.awaitTermination()
    }

    def process(source: DataFrame): DataFrame = {

      val calculateService = new CalculateService(60 * 10)
      source
        .as[Device]
        .withWatermark("t", "5 minutes")
        .groupByKey(_.id)
        .flatMapGroupsWithState(
          outputMode = OutputMode.Update(),
          timeoutConf = GroupStateTimeout.NoTimeout)(func = calculateService.calculate)
        .repartition($"id").toDF()

    }


    def cumulativeWrite(cumulativeValue: DataFrame, batchId: Long): Unit = {

      // TODO 正常数据写入kafka异常数据写入异常库
      cumulativeValue.show(false)
    }

    def otherWrite(otherValues: DataFrame, batchId: Long): Unit = {

      // TODO 其他数据写入Kafka
      otherValues.show(false)

    }


  }

  case class StateInfo(value: Double, exceptionStart: Long, exceptionEnd: Long)

  type State = (Long, StateInfo)
  implicit val State: Encoder[util.TreeMap[Long, StateInfo]] = org.apache.spark.sql.Encoders.kryo[util.TreeMap[Long, StateInfo]]

  case class Device(gatewayId: String, namespace: String, pointId: String, id: String, v: Double, s: String, t: Long)

  case class DeviceWithException(gatewayId: String, namespace: String, pointId: String, id: String, v: Double, s: String, t: Long, exceptionStart: Long, exceptionEnd: Long, outputMode:Int)


  class CalculateService(waterMark: Long = 60 * 10) extends Serializable {
    //todo
    def calculate(id: String, devices: Iterator[Device], state: GroupState[State]): Iterator[DeviceWithException] = {
      // Get the historical state from spark
      //val updateDeviceWithRanges = ListBuffer[DeviceWithException]()
      val updateDeviceWithRanges = mutable.TreeMap[Long, DeviceWithException]()

      val deviceList = devices.to[ListBuffer].sortBy(_.t)
      //println(state.getOption.isDefined)
      deviceList.foreach(device => {
        val cur_value = device.v
        val cur_time = device.t
        var isLastException, isCurrentException = false
        var oldState:State = (0, StateInfo(0, 0, 0))
        println(state)

        if(state.getOption.isDefined){
          if(device.t > state.get._1){
            val prev_value = state.get._2.value
            isLastException = state.get._2.exceptionStart > 0
            isCurrentException = cur_value - prev_value < 0

            if(isLastException){
              if(isCurrentException){
                val dev = DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, device.v, device.s, device.t, state.get._2.exceptionStart, 0, ExceptionWriteMode.INSERT)
                updateDeviceWithRanges(device.t) = dev
                oldState = (cur_time, StateInfo(cur_value, state.get._2.exceptionStart, 0))
              }else{
                val dev_last = DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, state.get._2.value, device.s, state.get._1, state.get._2.exceptionStart, device.t, ExceptionWriteMode.UPDATE)
                updateDeviceWithRanges(dev_last.t) = dev_last
                val dev_cur = DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, device.v, device.s, device.t, 0, 0, ExceptionWriteMode.NO_WRITE)
                updateDeviceWithRanges(dev_cur.t) = dev_cur
                oldState = (cur_time, StateInfo(cur_value, 0, 0))
              }
            }else{
              if(isCurrentException){
                val dev = DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, device.v, device.s, device.t, device.t, 0, ExceptionWriteMode.INSERT)
                updateDeviceWithRanges(dev.t) = dev
                oldState = (cur_time, StateInfo(cur_value, device.t, 0))
              }else{
                val dev = DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, device.v, device.s, device.t, 0, 0, ExceptionWriteMode.NO_WRITE)
                updateDeviceWithRanges(dev.t) = dev
                oldState = (cur_time, StateInfo(cur_value, 0, 0))
              }
            }
            state.update(oldState)
          }

        }else{
          oldState = (cur_time,StateInfo(cur_value,0,0))
          state.update(oldState)
          val dev = DeviceWithException(device.gatewayId, device.namespace, device.pointId, device.id, device.v, device.s, device.t, 0, 0, ExceptionWriteMode.NO_WRITE)
          updateDeviceWithRanges(dev.t) = dev
        }

      })

      // 状态更新到Spark
        updateDeviceWithRanges.values.toIterator
    }
  }


}
