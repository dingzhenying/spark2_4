package dc.streaming.processing

import java.util
import java.util.UUID

import dc.common.hbase.HBaseUtil
import dc.streaming.common.KafkaDataWithRuleReader
import dc.streaming.util.GetPropertieInfoUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

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
object MinuteValue {
  def catalog: String =
    s"""{
       |"table":{"namespace":"default", "name":"tableTest"},
       |"rowkey":"key",
       |"columns":{
       |"rowKey":{"cf":"rowkey", "col":"key", "type":"string"},
       |"v":{"cf":"info", "col":"v", "type":"double"},
       |"t":{"cf":"info", "col":"t", "type":"long"}
       |}
       |}""".stripMargin


  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .master("local[2]")
    //.config("spark.sql.shuffle.partitions", 1000)
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.sql.codegen.wholeStage", value = false)
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.codegen.wholeStage", value = false)

  def main(args: Array[String]): Unit = {
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
//        kafkaBootstrapServers= " microservice-dc-1:9092"
//        maxOffsetsPerTrigger="1000"
//        inputKafkaTopicName="dis-upper-lower-limits"
//        ruleServiceApi="http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_normal_by_minute"
//        watermarkDelayThreshold = "1 minutes"
//        awaitTermination="60000"
//        hbaseRegionsNumber =GetPropertieInfoUtil.getParameterValue("hbase_regions_number")
//        hbaseZookeeperQuorum ="114.116.53.121:2181,114.115.158.161:2181,114.115.151.0:2181"
//        hbaseZookeeperQuorum ="cdh-slave3:2181,cdh-slave2:2181,cdh-slave1:2181"
//        hbaseTableName = "tableTest"

    var checkpointLocation =
      if (args.length > 9) args(9) else "./tmp/temporary-MinuteValue-" + UUID.randomUUID.toString

    checkpointLocation="hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/MinuteValue/"+UUID.randomUUID.toString
    //绑定规则获取数据
    val reader = new KafkaDataWithRuleReader(spark)
      // kafka servers
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      //最大偏移量
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      // 输入topic
      .option("subscribe", inputKafkaTopicName)
      .option("dc.cleaning.rule.service.api", ruleServiceApi)
      .option("failOnDataLoss", "false")
    while (true) {
      val source = reader.load()
      val cleanData = source
      .filter($"id".isNotNull)

      val minDataStart = minDataCleaning(cleanData)

      while (minDataStart.status.isTriggerActive) {}
      minDataStart.awaitTermination(awaitTermination.toInt)
      minDataStart.stop()

    }

    def minDataCleaning(data: DataFrame): StreamingQuery = {
      //目标数据集获取做窗口聚合
      data
        .writeStream
        .outputMode("append")
        .foreachBatch((dataFrame: DataFrame, batchId: Long) => {
          val df = dataFrame.persist()
          df.printSchema()
          df.show(false)
          //缓存
          val minValue = cleanDBDataFream(df, hbaseRegionsNumber.toInt)
          minValue.printSchema()
          minValue.show(false)
          //wirterHbase(minValue)

          //写入清洗库方法2
//          minValue.write
//          .options(Map(HBaseTableCatalog.tableCatalog -> catalog,HBaseTableCatalog.newTable -> "5"))
//          .format("org.apache.spark.sql.execution.datasources.hbase")
//          .save()
          df.unpersist() //清除缓存
        })
        .option("checkpointLocation", checkpointLocation)
        .start()
    }

    def wirterHbase(minValue: DataFrame) = {
      val columnFamilyName: String = "info"
      val puts = new util.ArrayList[Put]()

      minValue.foreach(row => {
        val key = row.getAs[String]("rowKey")
        val p = new Put(Bytes.toBytes(key))
        val columnV = row.getAs[Double]("v")
        val columnT = row.getAs[Long]("t")
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
    }

    def cleanDBDataFream(df: DataFrame, numRegions: Int): DataFrame = {
      val rowKey = udf {
        (uri: String, t: String) => {
          val timestamp = t.toLong
          HBaseUtil.getHashRowkeyWithSalt(uri, timestamp, numRegions)
        }
      }
      val cleanData = df
        .groupBy(window($"eventTime", "1 minute", "1 minute"), $"uri") //滚动窗口前闭后开
        .agg(last("v").as("v"))
        .repartition($"uri")
        .withColumn("t", unix_timestamp($"window.end", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("rowKey", rowKey($"uri", $"t"))
        .withWatermark("eventTime", watermarkDelayThreshold)
        .select("rowKey", "v", "t")
      cleanData
    }
  }


  case class MeasurePoint(rowKey: String, v: Double, t: Long)


}
