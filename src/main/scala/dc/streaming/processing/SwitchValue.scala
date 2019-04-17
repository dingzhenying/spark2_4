package dc.streaming.processing

import java.util.UUID
import dc.streaming.common.KafkaDataWithRuleReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * 192.168.66.194:9092 subscribe dc-data http://192.168.66.194:8088/rulemgr/ruleInstanceWithBindByCode?code=dis_upper_lower_limits hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/limit-discriminate/
  */
object SwitchValue {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage:<bootstrap-servers> <subscribe-type> <topics> [<checkpoint-location>]")
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
      .option("dc.cleaning.rule.column.switch_default", "$.0.value")
      // 规则服务提供的api,参数为规则模板code
      .option("dc.cleaning.rule.service.api", ruleServiceAPI)
      .option("startingOffsets","earliest")//for test

    while (true) {
      val source = reader.load()

      val query = process(source).writeStream.outputMode("update")
        .foreachBatch(writeBatch _)
        //.option("checkpointLocation", checkpointLocation)
        .start()

      Thread.sleep(waitRestartTime)
      query.stop()
      query.awaitTermination(1000 * 60 * 5)
    }

    def process(source: DataFrame): DataFrame = {
      val switchCheckUDF = udf(switchCheck _)
      source.select($"*", switchCheckUDF($"v", $"switch_default") as "status")
    }

    //value、default自动转成int
    def switchCheck(value: Int, default: Int): Boolean = value == default
    //for test
    //def switchCheck(value: Int, default: Int): Boolean = value*default==value


    def writeBatch(dataFrame: DataFrame, batchId: Long): Unit = {
     // val dfCache = dataFrame.persist()
     // val normalValues=dfCache.filter($"status".equalTo(true))
      val exceptionValues = dataFrame.filter($"status".equalTo(false))
      val otherValues = dataFrame.filter($"status".isNull or $"status".equalTo(true))
      exceptionWrite(exceptionValues, batchId)
      otherWrite(otherValues, batchId)

      //dfCache.unpersist()
    }

    def exceptionWrite(exceptionValues: DataFrame, batchId: Long): Unit = {

      // TODO 异常数据写入异常库
      exceptionValues.show(1000,false)
    }

    def otherWrite(otherValues: DataFrame, batchId: Long): Unit = {

      // TODO 其他数据写入Kafka交给下一个流程去处理
      otherValues.show(1000,false)

    }

  }
}
