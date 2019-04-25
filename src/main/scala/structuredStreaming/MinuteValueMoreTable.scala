package dc.streaming.processing

import java.sql.Date
import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import dc.common.hbase.HBaseUtil
import dc.streaming.common.KafkaDataWithRuleReader
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Dingzhenying on 2019/2/12
  *
  * 按分钟取值
  *
  * microservice-dc-1:9092
  * dis-upper-lower-limits
  * 1000
  * http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_avg_by_minute
  * "10 minutes"
  * 600000
  * 100
  * cdh-worker-dc-1:2181,cdh-worker-dc-2:2181,cdh-worker-dc-3:2181
  * fxtest:dc_cleaning
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming-test/checkpoint/MinuteValueMoreTable/
  *
  *
  */
object MinuteValueMoreTable {

  def main(args: Array[ String ]): Unit = {
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

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.debug.maxToStringFields", 100)
      .config("spark.sql.codegen.wholeStage", value = false)
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.codegen.wholeStage", value = false)

    var checkpointLocation =
      if (args.length > 9) args(9) else "./tmp/temporary-MinuteValueMoreTable-" + UUID.randomUUID.toString

    checkpointLocation = "hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/MinuteValueMoreTable/" + UUID.randomUUID.toString
    //绑定规则获取数据

    val reader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputKafkaTopicName)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      //      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()
    val ruleOptions = Map[ String, String ]()

    while (true) {
      val source = KafkaDataWithRuleReader.joinRules(reader, ruleServiceApi, ruleOptions,markSystemTime = true)

      val cleanData = source
        .filter($"id".isNotNull)
//        .filter($"uri"==="/Hollysys_Bewg_modelNew/5c9634adffa1b100019af6ad")
      val minDataStart = minDataCleaning(cleanData)
      while (minDataStart.status.isTriggerActive) {}
      minDataStart.awaitTermination(awaitTermination.toInt)
      minDataStart.stop()
    }

    def minDataCleaning(data: DataFrame): StreamingQuery = {
      val hbaseTableList: Array[ String ] = HBaseUtil.listTableNames(hbaseZookeeperQuorum).map(_.getNameAsString)

      //目标数据集获取做窗口聚合
      data
        .writeStream
        .outputMode("update")
        .foreachBatch((dataFrame: DataFrame, batchId: Long) => {
          val df = dataFrame.persist()
          //          df.printSchema()
          df.show(false)
          //缓存
          val minValue = cleanDBDataFream(df, hbaseRegionsNumber.toInt)
          //          minValue.printSchema()
          minValue.show(false)
          wirterHbase(minValue, hbaseTableList)

          df.unpersist() //清除缓存
        })
        .option("checkpointLocation", checkpointLocation)
        .start()
    }

    def wirterHbase(minValue: DataFrame, hbaseTableList: Array[ String ]): Unit = {
      import org.springframework.util.LinkedMultiValueMap
      val columnFamilyName: String = "c"
      minValue.foreachPartition(rows => {
        val linkedMultiValueMap = new LinkedMultiValueMap[ String, Put ]
        rows.foreach(row => {
          val key = row.getAs[ String ]("rowKey")
          val p = new Put(Bytes.toBytes(key))
          val columnV = row.getAs[ Double ]("v")
          val columnT = row.getAs[ Long ]("t")
          val columnST = row.getAs[ Long ]("st")
          val hbaseTimeTableName = getTimeTable(hbaseTableName, columnT)
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
          p.addColumn(
            Bytes.toBytes(columnFamilyName),
            Bytes.toBytes("st"),
            Bytes.toBytes(columnST)
          )
          if (hbaseTimeTableName.contains(hbaseTimeTableName)) {
            linkedMultiValueMap.add(hbaseTimeTableName, p)
          }
        })
        import scala.collection.JavaConverters._

        linkedMultiValueMap.asScala.foreach(data => {
          HBaseUtil.putRows(hbaseZookeeperQuorum, data._1, new util.ArrayList[ Put ](data._2))
        })
        linkedMultiValueMap.clear()
      })
    }

    def getTimeTable(tableName: String, unTime: Long): String = {
      val simpleDateFormat = new SimpleDateFormat("yyyy")
      val date = new Date(unTime)
      val yearDate = simpleDateFormat.format(date)
      tableName + "_" + yearDate
    }

    def cleanDBDataFream(df: DataFrame, numRegions: Int): DataFrame = {
      val rowKey = udf {
        (uri: String, t: String) => {
          val timestamp = t.toLong
          HBaseUtil.getHashRowkeyWithSalt(uri, timestamp, numRegions)
        }
      }
      val cleanData = df
        .withWatermark("wt", watermarkDelayThreshold)
        .groupBy(window($"wt", "1 minute", "1 minute"), $"uri") //滚动窗口前闭后开
        .agg(last("v").as("v"),min("st").as("st"))
        .repartition($"uri")
        .withColumn("t", unix_timestamp($"window.end", "yyyy-MM-dd HH:mm:ss") * 1000)
        .withColumn("rowKey", rowKey($"uri", $"t"))
        .select("rowKey", "v", "t","st")
      cleanData
    }
  }

  case class MeasurePoint(rowKey: String, v: Double, t: Long,st:Long)


}
