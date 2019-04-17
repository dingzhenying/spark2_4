package dc.streaming.processing

import java.util
import java.util.UUID

import dc.common.hbase.HBaseUtil
import dc.streaming.common.KafkaDataWithRuleReader
import dc.streaming.util.GetPropertieInfoUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Dingzhenying on 2019/2/14
  *
  * 按分钟取平均值
  *
  * microservice-dc-1:9092
  * dc-out-test-1
  * 1000
  * http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_avg_by_minute
  * "1 hours"
  * 6000000
  * 114.116.53.121:2181,114.115.158.161:2181,114.115.151.0:2181
  * 100
  * tableTest
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/MinuteAvgValue/
  *
  */
object MinuteAvgValue {
  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .config("spark.sql.shuffle.partitions", 10)
    .config("spark.debug.maxToStringFields", 100)
    //.master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.codegen.wholeStage", value = false)

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      System.err.println("Usage:"+
        "<kafkaBootstrapServers> <inputKafkaTopicName> <maxOffsetsPerTrigger>" +
        "<ruleServiceApi> <watermarkDelayThreshold> <awaitTermination> " +
        "<hbaseRegionsNumber> <hbaseZookeeperQuorum> <hbaseTableName>" +
        "[<checkpoint-location>]")
      System.exit(1)
    }
    val Array(
    kafkaBootstrapServers,
    inputKafkaTopicName,
    maxOffsetsPerTrigger,
    ruleServiceApi,
    watermarkDelayThreshold,
    awaitTermination,
    hbaseRegionsNumber,
    hbaseZookeeperQuorum,
    hbaseTableName,
    _*) = args

//    kafkaBootstrapServers= "114.115.238.197:9092"
//    maxOffsetsPerTrigger="1000"
//    inputKafkaTopicName="dc-out-test-1"
//    ruleServiceApi="http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_normal_by_minute"
//    watermarkDelayThreshold = "1 hours"
//    awaitTermination="60000"
//    hbaseRegionsNumber =GetPropertieInfoUtil.getParameterValue("hbase_regions_number")
//    hbaseZookeeperQuorum ="114.116.53.121:2181,114.115.158.161:2181,114.115.151.0:2181"
//    hbaseTableName = "tableTest"


    val checkpointLocation =
      if (args.length > 9) args(9) else "/tmp/temporary-MinuteAvgValue-" + UUID.randomUUID.toString

    //绑定规则获取数据
    val reader = new KafkaDataWithRuleReader(spark)
      // kafka servers
      .option("kafka.bootstrap.servers",kafkaBootstrapServers)
      //最大偏移量
      .option("maxOffsetsPerTrigger",maxOffsetsPerTrigger)
      // 输入topic
      .option("subscribe",inputKafkaTopicName)
      // 规则服务提供的api,参数为规则模板code
      .option("dc.cleaning.rule.service.api",ruleServiceApi)

    while (true) {
      val source = reader.load()
      val cleanData = source.filter($"id".isNotNull)
      val minDataStart = cleanData
        .writeStream
        .outputMode("update")
        .foreachBatch((dataFrame: DataFrame, batchId: Long) => {
          val df = dataFrame.persist()
          //缓存
          val minValue = cleanDBDataFream(df,hbaseRegionsNumber.toInt)
          minValue.show(false)
          wirterHbase(minValue, hbaseZookeeperQuorum, hbaseTableName)
          df.unpersist() //清除缓存
        })
        .option("checkpointLocation", checkpointLocation)
        .start()
      while (minDataStart.status.isTriggerActive) {}
      minDataStart.awaitTermination(awaitTermination.toInt)
      minDataStart.stop()
    }

    def wirterHbase(minValue: DataFrame, hbaseZookeeperQuorum: String, hbaseTableName: String): Unit = {
      val columnFamaliyName: String = "info"
      val puts = new util.ArrayList[Put]()
      minValue.foreach(row => {
        val key = row.getString(2)
        val p = new Put(Bytes.toBytes(key))
        val columnV = row.getDouble(0)
        val columnT = row.getLong(1)
        p.addColumn(
          Bytes.toBytes(columnFamaliyName),
          Bytes.toBytes("v"),
          Bytes.toBytes(columnV)
        )
        p.addColumn(
          Bytes.toBytes(columnFamaliyName),
          Bytes.toBytes("t"),
          Bytes.toBytes(columnT)
        )
        puts.add(p)
      })
      HBaseUtil.putRows(hbaseZookeeperQuorum, hbaseTableName, puts)
    }
    def cleanDBDataFream(df: DataFrame, numRegions: Int) = {
      val rowKey = udf {
        (uri: String, t: String) => {
          val timestamp = t.toLong
          HBaseUtil.getHashRowkeyWithSalt(uri, timestamp, numRegions)
        }
      }
//      df.show(false)
      val caleanData = df.groupBy(window($"eventTime", "1 minute", "1 minute"), $"uri") //滚动窗口前闭后开
        .agg(avg("v").as("v"))
        .repartition($"uri")
        .withColumn("t", unix_timestamp($"window.end", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("rowKey", rowKey($"uri", $"t"))
        //设置时间水印
        .withWatermark("eventTime", watermarkDelayThreshold)
        .drop("window", "uri")

      caleanData.printSchema()
      caleanData
    }
  }




  case class MeasurePoint(rowKey: String, v: Double, t: Long)

}
