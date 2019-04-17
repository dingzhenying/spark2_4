package dc.streaming.processing

import java.util.UUID

import dc.streaming.common.KafkaDataWithRuleReader
import org.apache.spark.sql.{DataFrame,SparkSession}

/**
  * @author : shirukai
  * @date : 2019-02-12 10:22
  *       上下限判别
  *       192.168.66.194:9092 subscribe dc-data http://192.168.66.194:8088/rulemgr/ruleInstanceWithBindByCode?code=shxxpb hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/limit-discriminate/
  */
object LimitDiscriminate {
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
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 实例化Reader
    val reader = new KafkaDataWithRuleReader(spark)
      // kafka servers
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("maxOffsetsPerTrigger", "1000")
      // topic
      .option(subscribeType, topics)
      // 规则列 key为:dc.cleaning.rule.column.列名 value为：规则模板json中的位置
      .option("dc.cleaning.rule.column.upperbound", "$.0.value")
      .option("dc.cleaning.rule.column.lowerbound", "$.1.value")
      // 规则服务提供的api,参数为规则模板code
      .option("dc.cleaning.rule.service.api", ruleServiceAPI)

    while (true) {
      val source = reader.load()

      val query = process(source).writeStream.outputMode("update")
        .foreachBatch(writeBatch _)
        .option("checkpointLocation", checkpointLocation)
        .start()

      Thread.sleep(waitRestartTime)
      query.stop()
      query.awaitTermination(1000 * 60 * 5)
    }

    def process(source: DataFrame): DataFrame = {
      val limitDiscriminateUDF = udf(limitDiscriminate _)
      source.select($"*", limitDiscriminateUDF($"v", $"lowerbound", $"upperbound").as("status"))
    }

    def writeBatch(dataFrame: DataFrame, batchId: Long): Unit = {
      val dfCache = dataFrame.persist()
      val exceptionValues = dfCache.filter($"status".equalTo(false))
      val otherValues = dfCache.filter($"status".isNull or $"status".equalTo(true))

      exceptionWrite(exceptionValues, batchId)
      otherWrite(otherValues, batchId)

      dfCache.unpersist()
    }

    def exceptionWrite(exceptionValues: DataFrame, batchId: Long): Unit = {

      // TODO 异常数据写入异常库
      exceptionValues.show(false)
    }

    def otherWrite(otherValues: DataFrame, batchId: Long): Unit = {

      // TODO 其他数据写入Kafka
      otherValues.show(false)

    }

    def limitDiscriminate(value: Double, lower: Double, upper: Double): Boolean = simpleDiscriminate(value, lower, upper)

    def simpleDiscriminate(value: Double, lower: Double, upper: Double): Boolean = {
      lower < value && value < upper
    }
  }
}
