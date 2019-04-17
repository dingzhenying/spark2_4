package dc.streaming.processing

import java.util.UUID

import dc.common.hbase.HBaseUtil
import dc.streaming.common.KafkaDataWithRuleReader
import dc.streaming.processing.MinuteAvgValue_foreach.spark
import dc.streaming.sink.HBaseForeachWriter
import dc.streaming.util.GetPropertieInfoUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Dingzhenying on 2019/2/12
  *
  * 按分钟取值_foreach
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
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/MinuteValue_foreach/
  *
  *
//        kafkaBootstrapServers= "114.115.238.197:9092"
//        maxOffsetsPerTrigger="1000"
//        inputKafkaTopicName="dc-out-test-1"
//        ruleServiceApi="http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_normal_by_minute"
//        watermarkDelayThreshold = "1 hours"
//        awaitTermination="60000"
//        hbaseRegionsNumber =GetPropertieInfoUtil.getParameterValue("hbase_regions_number")
//        hbaseZookeeperQuorum ="114.116.53.121:2181,114.115.158.161:2181,114.115.151.0:2181"
//        hbaseTableName = "tableTest"
  *
  */
object MinuteValue_foreach {

  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    //.master("local[2]")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.sql.codegen.wholeStage", value = false)
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      System.err.println("Usage:"+
        "<kafkaBootstrapServers> <inputKafkaTopicName> <maxOffsetsPerTrigger>" +
        "<ruleServiceApi> <watermarkDelayThreshold> <awaitTermination> " +
        "<hbaseRegionsNumber> <hbaseZookeeperQuorum> <hbaseTableName>" +
        "[<checkpoint-location>]")
      System.exit(1)
    }
    var Array(
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



    val checkpointLocation =
      if (args.length > 9) args(9) else "/tmp/temporary-MinuteValue_foreach-" + UUID.randomUUID.toString

    //绑定规则获取数据
    val reader = new KafkaDataWithRuleReader(spark)
      // kafka servers
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      //最大偏移量
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      // 输入topic
      .option("subscribe", inputKafkaTopicName)
      // 规则服务提供的api,参数为规则模板code
      .option("dc.cleaning.rule.service.api", ruleServiceApi)
    //      .option("failOnDataLoss", "false")
    while (true) {
      val source = reader.load()
      val cleanData = source.filter($"id".isNotNull)

      val minDataStart = minData2HbaseForeach(cleanData,hbaseZookeeperQuorum,hbaseTableName)

      while (minDataStart.status.isTriggerActive) {}
      minDataStart.awaitTermination(awaitTermination.toInt)
      minDataStart.stop()

    }

    def minData2HbaseForeach(data: DataFrame, hbaseZookeeperQuorum: String, hbaseTableName: String) = {
      //目标数据集获取做窗口聚合
      cleanDBDataFream(data, hbaseRegionsNumber.toInt)
        .as[MeasurePoint]
        .writeStream
        .foreach(
          new HBaseForeachWriter[MeasurePoint] {
            override val hbase_zookeeper_quorum: String = hbaseZookeeperQuorum //GetPropertieInfoUtil.getParameterValue("hbase_zookeeper_quorum")

            override val tableName: String = hbaseTableName //GetPropertieInfoUtil.getParameterValue("tableName")

            override val hbaseConfResources: Seq[String] = Seq("core-site.xml", "hbase-site.xml")

            override def toPut(record: MeasurePoint): Put = {
              val columnFamaliyName: String = "info"
              val key = record.rowKey
              val p = new Put(Bytes.toBytes(key))
              val columnV = record.v
              val columnT = record.t
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
              p
            }
          })
        .outputMode("update")
        .option("checkpointLocation", checkpointLocation)
        .start()
    }

    def cleanDBDataFream(df: DataFrame, numRegions: Int): DataFrame = {
      val rowKey = udf {
        (uri: String, t: String) => {
          val timestamp = t.toLong
          HBaseUtil.getHashRowkeyWithSalt(uri, timestamp, numRegions)
        }
      }
      df
        .withWatermark("eventTime", watermarkDelayThreshold)
        .groupBy(window($"eventTime", "1 minute", "1 minute"), $"uri") //滚动窗口前闭后开
        .agg(last("v").as("v"))
        .repartition($"uri")
        .withColumn("t", unix_timestamp($"window.end", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("rowKey", rowKey($"uri", $"t"))
        .select("rowKey", "v", "t")

    }
  }

  case class MeasurePoint(rowKey: String, v: Double, t: Long)


}
