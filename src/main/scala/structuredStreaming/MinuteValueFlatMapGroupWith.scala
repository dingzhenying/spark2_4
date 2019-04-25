package structuredStreaming

import java.util
import java.util.{Date, UUID}

import dc.common.hbase.HBaseUtil
import dc.streaming.common.KafkaDataWithRuleReader
import org.apache.commons.httpclient.util.DateUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Dingzhenying on 2019/2/12
  *
  * 按分钟取值
  *
  * microservice-dc-1:9092
  * dc-out-test-1
  * 1000
  * http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_normal_by_minute
  * "1 hours"
  * 6000000
  * 114.116.53.121:2181,114.115.158.161:2181,114.115.151.0:2181
  * 100
  * tableTest
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/MinuteValue/
  *
  *
  */
object MinuteValueFlatMapGroupWith {

  def main(args: Array[ String ]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.sql.codegen.wholeStage", value = false)
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.codegen.wholeStage", value = false)

    if (args.length < 9) {
      System.err.println("Usage:" +
        "<kafkaBootstrapServers> <inputKafkaTopicName> <maxOffsetsPerTrigger>" +
        "<ruleServiceApi> <watermarkDelayThreshold> <awaitTermination> " +
        "<hbaseRegionsNumber> <hbaseZookeeperQuorum> <hbaseTableName>" +
        "[<checkpoint-location>]")
      System.exit(1)
    }

    var Array(
    kafkaBootstrapServers, inputKafkaTopicName, maxOffsetsPerTrigger,
    ruleServiceApi, watermarkDelayThreshold, awaitTermination,
    hbaseRegionsNumber, hbaseZookeeperQuorum, hbaseTableName, _*) = args
    println(args.toList)

    var checkpointLocation =
      if (args.length > 9) args(9) else "./tmp/temporary-MinuteValueFlatMapGroupWith-" + UUID.randomUUID.toString

    checkpointLocation="hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/MinuteValueFlatMapGroupWith2/"+UUID.randomUUID.toString
    //绑定规则获取数据
    val kafkaData = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputKafkaTopicName)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .option("failOnDataLoss", "true")
      .load()
    while (true) {
      val source = kafkaData
      val cleanData = KafkaDataWithRuleReader
        .joinRules(source, ruleServiceApi, Map[ String, String ]())

      val minDataStart = minDataCleaning(cleanData)


      val outData=minDataStart
        .writeStream
        .outputMode("update")
//        .foreachBatch((dataFrame: DataFrame, batchId: Long) => {
//          val df = dataFrame.persist()
//          df.printSchema()
//          df.show(false)
//          //缓存
////          val minValue = cleanDBDataFream(df, hbaseRegionsNumber.toInt)
//          //  minValue.printSchema()
//          //  minValue.show(false)
////          writeHbase(minValue)
//          df.unpersist() //清除缓存
//        })
        .format("console")
        .option("truncate", false)
        .start()
      while (outData.status.isTriggerActive) {}
      outData.awaitTermination(awaitTermination.toInt)
      outData.stop()

    }

    def minDataCleaning(data: DataFrame) = {
      //目标数据集获取做窗口聚合
      data.printSchema()
      data
        .filter($"id".isNotNull)
//        .filter($"id"==="/SymLink-bewg-YanLiangWSC/PLC01_300.S_PLC1.S_HIGHTP_MIXM2_LR")
//        .withColumn("t2", from_unixtime($"t" / 1000).cast(TimestampType))
        .withColumn("t3", from_unixtime($"t" / 1000, "yyyy-MM-dd HH:mm"))
        .as[ DataInfo ]
        .withWatermark("wt", "1 minutes")
        .groupByKey(_.pointId)
        .flatMapGroupsWithState(
          outputMode = OutputMode.Update(),
          timeoutConf = GroupStateTimeout.EventTimeTimeout())(func = calculate)

//        .as[outData]
    }

    def writeHbase(minValue: DataFrame) = {
      val columnFamilyName: String = "c"
      minValue.foreachPartition(rows => {
        val puts = new util.ArrayList[ Put ]
        rows.foreach(row => {
          val key = row.getAs[ String ]("rowKey")
          val p = new Put(Bytes.toBytes(key))
          val columnV = row.getAs[ Double ]("v")
          val columnT = row.getAs[ Long ]("t")
          p.addColumn(
            Bytes.toBytes(columnFamilyName),
            Bytes.toBytes("v"),
            Bytes.toBytes(columnV)
          )
          p.addColumn(
            Bytes.toBytes(columnFamilyName),
            Bytes.toBytes("t"),
            Bytes.toBytes(columnT)
          )
          puts.add(p)
        })
        HBaseUtil.putRows(hbaseZookeeperQuorum, hbaseTableName, puts)
      })
    }
    def cleanDBDataFream(df: DataFrame, numRegions: Int): DataFrame = {
      val rowKey = udf {
        (uri: String, t: String) => {
          val timestamp = t.toLong
          HBaseUtil.getHashRowkeyWithSalt(uri, timestamp, numRegions)
        }
      }
      df
        .withWatermark("wt", watermarkDelayThreshold)
        .groupBy(window($"wt", "1 minute", "1 minute"), $"uri") //滚动窗口前闭后开
        .agg(last("v").as("v"))
        .repartition($"uri")
        .withColumn("t", unix_timestamp($"window.end", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("rowKey", rowKey($"uri", $"t"))
        .select("rowKey", "v", "t")
    }

  }
  //旧数据
  case class Device(pointId: String, t: String)

  //旧状态类型
  type State = mutable.Map[Device, ListBuffer[Double]]

  //输出数据
  case class DataInfo(id:String,uri:String,gatewayId: String, namespace: String, pointId: String, t: String, v: String, s: String, wt: String, t3: String)

  case class outData(id:String,uri:String,gatewayId: String, namespace: String, pointId: String, t: String, sum_v: Double, avg_v: Double, last_v: Double, s: String)

  //迭代器
  def calculate(id: String, inData: Iterator[ DataInfo ], oldState: GroupState[ State ]) = {
    val updateDeviceWithRanges: mutable.TreeMap[ String, outData ] = new mutable.TreeMap[ String, outData ]
    val oldKey=oldState.getCurrentWatermarkMs()
    println(oldKey+"----"+new Date(oldKey)+"---"+DateUtil.formatDate(new Date(oldKey),"yyyy-MM-dd HH:mm:ss") )
    //历史状态
    var olderData: State = oldState.getOption.getOrElse(mutable.Map[ Device, ListBuffer[ Double ] ](Device("", "") -> new ListBuffer[ Double ]))
//    oldState.setTimeoutDuration()
    inData.foreach(data => {
      var oldValue: ListBuffer[ Double ] = olderData.getOrElse(Device(data.pointId, data.t3), new ListBuffer[ Double ])
      //添加参数
      oldValue.+=(data.v.toDouble)
      olderData += (Device(data.pointId, data.t3) -> oldValue)

      val sumData: Double = oldValue.sum
      val avgData: Double = sumData / oldValue.size
      val lastData: Double = oldValue.last
      //println("sum:"+sumData+" size:"+oldValue.size+" avg:"+avgData+" lastData:"+lastData)
      val dataMap =new util.TreeMap[Long,String]()
      dataMap.headMap(100000).clear()
      updateDeviceWithRanges.put(data.t3, outData(data.id,data.uri,data.gatewayId, data.namespace, data.pointId, data.wt, sumData, avgData, lastData, data.s))
    })
    oldState.update(olderData)
    updateDeviceWithRanges.valuesIterator
  }


  case class MeasurePoint(rowKey: String, v: Double, t: Long)


}
