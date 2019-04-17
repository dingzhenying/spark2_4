package dc.streaming.processing

import java.util.UUID

import dc.streaming.common.{HBaseDataBatchWriter, KafkaDataWithRuleReader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{avg, window, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by shirukai on 2019-03-12 20:06
  * 按分钟取值写入HBase
  * microservice-dc-1:9092
  * dc-out-test-1
  * 1000
  * http://microservice-dc-1:8088/rulemgr/v1/ruleInstanceWithBindByCode?code=clean_normal_by_minute
  * "10 minutes"
  * 36000000
  * cdh-worker-dc-1:2181,cdh-worker-dc-2:2181,cdh-worker-dc-3:2181
  * 100
  * dc_dev:dc_cleaning
  * hdfs://cdh-master-dc-1:8020/tmp/dc-streaming/checkpoint/minute-value-2-hbase/
  */
object MinuteValue2HBase extends Logging{
  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println("Usage: MinuteAvg2HBase <input-bootstrap-servers> <input-topics> <max-offsets-per-trigger>" +
        " <rule-service-api> <watermark-delay-threshold> <wait-restart-time> <hbase-zookeeper-quorum> " +
        "<hbase-region-number> <hbase-table-name>[<checkpoint-location>]")
      System.exit(1)
    }

    val Array(inputBootstrapServers, inputTopics, maxOffsetsPerTrigger, ruleServiceAPI, watermarkDelayThreshold,
    waitRestartTime, hbaseZookeeperQuorum, hbaseRegionNumber, hbaseTableName, _*) = args
    val checkpointLocation =
      if (args.length > 9) args(9) else "/tmp/temporary-" + UUID.randomUUID.toString
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
//      .master("local[2]")
      //.config("spark.sql.shuffle.partitions", 10)
      .config("dfs.client.use.datanode.hostname", "true")
      .getOrCreate()

    import spark.implicits._
    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", inputBootstrapServers)
      .option("subscribe", inputTopics)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      //      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()
    val ruleOptions = Map[String, String]()

    while (true) {
      val stream = KafkaDataWithRuleReader.joinRules(source, ruleServiceAPI, ruleOptions)
      val minuteAvg = process(stream.filter($"id".isNotNull))
        .writeStream
        .outputMode("update")
        .foreachBatch(write2HBase _)
        .option("checkpointLocation", checkpointLocation)
        .start()

      Thread.sleep(waitRestartTime.toInt)
      while (minuteAvg.status.isTriggerActive) {}
      minuteAvg.stop()
      minuteAvg.awaitTermination()
      logWarning("====================query 正在重启=====================")
    }

    def process(df: DataFrame): DataFrame = {
      df.withWatermark("eventTime", watermarkDelayThreshold)
        .groupBy(window($"eventTime", "1 minute", "1 minute")
          .getItem("end").as("t"), $"uri")
        .agg(last("v").as("v"))
    }


    def write2HBase(dataFrame: DataFrame, batchId: Long): Unit = {
//      dataFrame.show(false)
      HBaseDataBatchWriter.save(dataFrame, batchId, hbaseZookeeperQuorum, hbaseRegionNumber.toInt, hbaseTableName)
    }


  }


}

