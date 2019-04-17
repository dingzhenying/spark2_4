package structuredStreaming

import com.spark.sparkStreaming.CumulativeAmountDiscrimination.schema
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types._
import structuredStreaming.flatMapGroupsWithState.getOldEventTimeValue

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Dingzhenying on 2019/3/27
  */
object flatMapGropsWithStateTest {
  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", 10)
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val kafkaData = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "microservice-dc-1:9092")
      .option("subscribe", "test_dis-upper-lower-limits")
      .option("failOnDataLoss", "false") //数据丢失f失败
      //.option("startingOffsets", "latest")
      .load()
    val data = kafkaData
      //.select(schema_of_json($"value"))
      .select(from_json($"value".cast(StringType), schema).as("value"))
      .select($"value.*")
      .filter($"pointId" === "JYJ.JYJ.PAC2_LL" || $"pointId" === "ns=1001;s=P13_AI_001.In_Channel0" || $"pointId" === "ChnOPC.AFY.AFY.AFY-FLOAT.TAG33")
      .withColumn("t2", from_unixtime($"t" / 1000).cast(TimestampType))
      //.withColumn("t3",unix_timestamp(from_unixtime($"t"/1000,"yyyy-MM-dd HH:mm"),"yyyy-MM-dd HH:mm"))
      .withColumn("t3", from_unixtime($"t" / 1000, "yyyy-MM-dd HH:mm"))
      .as[DataInfo]
      .withWatermark("t2", "10 minutes")

      .groupByKey(_.pointId)
      .flatMapGroupsWithState(
        outputMode = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.NoTimeout)(func = calculate)

    val write = data
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .start()
    write.awaitTermination()
    write.stop()
  }

  //旧数据
  case class Device(pointId: String, t: String)

  //旧状态类型
  type State = mutable.Map[Device, ListBuffer[Double]]

  //implicit val State: Encoder[State] = org.apache.spark.sql.Encoders.kryo[State]

  //输出数据
  case class DataInfo(gatewayId: String, namespace: String, pointId: String, t: String, v: String, s: String, t2: String, t3: String)

  case class outData(gatewayId: String, namespace: String, pointId: String, t: String, sum_v: Double, avg_v: Double, last_v: Double, s: String)

  //迭代器
  def calculate(id: String, inData: Iterator[DataInfo], oldState: GroupState[State]): Iterator[outData] = {
    val updateDeviceWithRanges = mutable.TreeMap[String, outData]()
    //历史状态
    var olderData: State = oldState.getOption.getOrElse(mutable.Map(Device("", "") -> ListBuffer(0)))
    println("Watermark:" + oldState.getCurrentWatermarkMs())
    //    var upDate:State =oldState.getOption.get
    inData.foreach(data => {
      var oldValue: ListBuffer[Double] = olderData.getOrElse(Device(data.pointId, data.t3), new ListBuffer[Double])
//      println("oldValue:"+oldValue)
      println(s"oldValue = ${oldValue}")
      //添加参数
      oldValue.+=(data.v.toDouble)
      olderData += (Device(data.pointId, data.t3) -> oldValue)
      //      println("传入数据：" + data)
      //      println("olderData：" + olderData)
      //      println("历史值：" + oldValue)

      val sumData: Double = oldValue.sum
      val avgData: Double = sumData / oldValue.size
      val lastData: Double = oldValue.last
      //println("sum:"+sumData+" size:"+oldValue.size+" avg:"+avgData+" lastData:"+lastData)

      updateDeviceWithRanges.put(data.t3, outData(data.gatewayId, data.namespace, data.pointId, data.t3, sumData, avgData, lastData, data.s))

    })
    oldState.update(olderData)
    println(oldState.getOption)
    updateDeviceWithRanges.values.toIterator
  }

  //  分钟取值
  val schema = StructType(
    List(
      StructField("gatewayId", StringType),
      StructField("namespace", StringType),
      StructField("pointId", StringType),
      StructField("t", StringType),
      StructField("v", StringType),
      StructField("s", StringType)
    )
  )
}
