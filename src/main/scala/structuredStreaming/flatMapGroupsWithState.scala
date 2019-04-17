package structuredStreaming

import java.util

import com.spark.sparkStreaming.CumulativeAmountDiscrimination.schema
import foreachBeach.splitData
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.functions.{from_json, from_unixtime, lit}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, TimestampType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Dingzhenying on 2019/2/19
  *
  * flatMapGroupsWithState,获取上一个事件时间的值,和下一个事件时间的值
  *
  */
object flatMapGroupsWithState {
  val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").getOrCreate()

  import spark.implicits._

  //  spark.conf.set("spark.sql.codegen.wholeStage", false)
  //  spark.conf.set("spark.debug.maxToStringFields", 1000)

  def main(args: Array[String]): Unit = {
    val read = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.66.194:9092")
      .option("subscribe", "dc-data")
      .option("failOnDataLoss", "false") //数据丢失f失败
      //.option("startingOffsets", "latest")
      .load()

    val data = read
      .select(from_json($"value".cast(StringType), schema).as("value"))
      .select("value.*")
      .withColumn("t", $"t".cast(LongType))
      .withColumn("t", from_unixtime($"t" / 1000, "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .withWatermark("t", "10 hours")
      .withColumn("v", splitData($"v", lit(2)).cast(DoubleType))
      .as[DataInfo]
      .groupByKey(_.pointId)
      .flatMapGroupsWithState(outputMode = OutputMode.Update(), timeoutConf = GroupStateTimeout.EventTimeTimeout())(func = getOldEventTimeValue)
    val writer = data
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)//控制台输出
      .start()
    writer.awaitTermination()
  }

  case class DataInfo(gatewayId: String, namespace: String, pointId: String, internalSeriesId: String, v: Double, s: String, t: Long)

  case class intoOldDataInfo(gatewayId: String, namespace: String, pointId: String, v: Double, s: String, t: Long, olderT: Long, olderValue: Double, nextT: Long, nextValue: Double)

  type State = mutable.Map[Long, DataInfo]

//  type State = (Long, DataInfo)
  implicit val State2: Encoder[util.TreeMap[Long, DataInfo]] = org.apache.spark.sql.Encoders.kryo[util.TreeMap[Long, DataInfo]]


  //保存时间戳和值
  //devices：当前的数据信息 state：历史数据状态   Iterator[DeviceWithRange]：返回的处理后数据
  def getOldEventTimeValue(id: String, devices: Iterator[DataInfo], state: GroupState[State]): Iterator[intoOldDataInfo] = {
    val start = System.currentTimeMillis()
   // state.setTimeoutDuration("1 hour")
    // 获取根据key值获取历史数据
//    val oldState:State = state.getOption.getOrElse(Long,DataInfo)
    val oldStates:State = mutable.Map (0L-> DataInfo("", "", "","",0,"",0))
//    val initialState: State = Map(id -> 0)
    val oldState=  state.getOption.getOrElse(oldStates)

    val updateDeviceWithRanges = ListBuffer[intoOldDataInfo]()
    // 对新数据按照时间戳排序
    val deviceList = devices.to[ListBuffer].sortBy(_.t) //获取根据时间t排序的获得有序的list[device]
    deviceList.foreach(device => {
      val currentTimestamp = device.t //获取当前数据的事件时间
      // 将当前值更新到历史状态
      oldState.put(device.t, device)
      val oldStatelist = oldState.to[ListBuffer].sortBy(_._1)

      //      oldStatelist ++= (oldState)
      var olderDatalist: ListBuffer[(Long, DataInfo)] = oldStatelist.filter(_._1 < currentTimestamp).sortBy(_._1)
      //获取list中满足条件的第一个值（此方法不满足使用条件）
//      var nextData = oldStatelist.sortBy(_._1).find(_._1 > currentTimestamp) //获取list中满足条件的第一个值（此方法不满足使用条件）
//
      if (olderDatalist.size > 0
//        || nextData.isDefined
      ) {
        var olderT: Long = 0
        var olderValue: Double = 0
        var nextT: Long = 0
        var nextValue: Double = 0
        if (olderDatalist.size > 0) {
          val upper = olderDatalist.last
          olderT = upper._1
          olderValue = upper._2.v
        }

//        if (nextData.isDefined) {
//          nextT = nextData.get._1
//          nextValue = nextData.get._2.v
//        }
        var newIntoOldDataInfo = intoOldDataInfo(device.gatewayId, device.namespace, device.pointId, device.v, device.s, device.t, olderT, olderValue, nextT, nextValue)


        // println(newIntoOldDataInfo)
        updateDeviceWithRanges.append(newIntoOldDataInfo)
        //println(upper.get._1)
      }

      //基于treeMap进行排序
      //      if(oldState.size>0){
      //        val upper= oldState.filter(_._1<currentTimestamp).last
      //        updateDeviceWithRanges.append(intoOldDataInfo(device.gatewayId, device.namespace, device.pointId, device.v,
      //                  device.s, device.t, upper._1,upper._2.v))
      //      }

    })

    // 状态更新到Spark
    state.update(oldState)
    val end = System.currentTimeMillis()
    println("处理时间：" + (end - start))
    updateDeviceWithRanges.toIterator
  }
}
