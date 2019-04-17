package structuredStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import it.unimi.dsi.fastutil.longs._

/**
  *
  * 累积量判别——flatMap
  *
  * Created by Dingzhenying on 2019/4/8
  */
object addDataCompare {
  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    println("start with appName " + this.getClass.getSimpleName)

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
      //      .withColumn("t3", from_unixtime($"t" / 1000, "yyyy-MM-dd HH:mm"))
      .as[DataInfo]
      .withWatermark("t2", "10 minutes")
      .groupByKey(_.pointId)
      .flatMapGroupsWithState(
        outputMode = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.NoTimeout)(func = addDataCompare)

    val writer = data
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .start()
    writer.awaitTermination(600000)
    writer.stop()
  }

  case class DataInfo(gatewayId: String, namespace: String, pointId: String, t: String, v: String, s: String, t2: String)

  case class outData(gatewayId: String, namespace: String, pointId: String, t: String, v: String, s: String, state: Boolean)


  //历史数据状态
  case class Device(pointId: String, t: String, v: Double)

  type State = mutable.TreeMap[Long, Device]

  val splitData = udf((data: String, int: Int) => {
    data.substring(int)
  })

  def addDataCompare(id: String, inData: Iterator[DataInfo], oldState: GroupState[State]): Iterator[outData] = {
    val historyState = new Long2DoubleRBTreeMap()

    val outDate = new ListBuffer[outData]
    val RamData = oldState.getOption.getOrElse(new mutable.TreeMap[Long, Device])
//    println("lastDataSize:" + RamData.size)
    val filterKey = oldState.getCurrentWatermarkMs() / 1000
//    println("========================="+filterKey)
    historyState.headMap(filterKey).clear()

//    historyState.clear(filterKey)
    // TODO: 获取数据集
    
    if (!oldState.exists) println("历史数据不存在！")

    inData.foreach(data => {
      //println(data)
      // TODO: 获取map最后的值
//      if (RamData.size > 0) {
//        val lastData = RamData.last
//        println("lastData:" + lastData)
//      }

      RamData.put(data.t.toLong, Device(data.pointId, data.t2, data.v.toDouble))
      //val thisData  =RamData.get(Device(data.pointId,data.t))
      // TODO: 获取上一个值
      // TODO: 判断大小
      outDate.+=(outData(data.gatewayId, data.namespace, data.pointId, data.t2, data.v, data.s, true))
    })
    oldState.update(RamData)

    outDate.toIterator
  }

  def getCompareState() = {

  }

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
