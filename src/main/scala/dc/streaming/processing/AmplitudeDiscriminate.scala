package dc.streaming.processing

import java.util.UUID

import dc.streaming.common.KafkaDataWithRuleReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author :
  * @date :
  *
  *       192.168.66.194:9092 subscribe dc-data http://192.168.66.194:8088/rulemgr/ruleInstanceWithBindByCode?code=shxxpb hdfs://192.168.66.192:8020/tmp/dc-streaming/checkpoint/limit-discriminate/
  */
object AmplitudeDiscriminate {



  def main(args: Array[String]): Unit = {

    val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString

    // 等待重启时间1小时
    val waitRestartTime = 1000 * 60 * 60 * 1

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val ruleServiceAPI = "http://192.168.66.194:8088/rulemgr/ruleInstanceWithBindByCode?code=fdpb"
    // 实例化Reader
    val reader = new KafkaDataWithRuleReader(spark)
      // kafka servers
      .option("kafka.bootstrap.servers", "192.168.66.194:9092")
      .option("maxOffsetsPerTrigger", "1000")
      // topic
      .option("subscribe", "dc-data")
      // 规则列 key为:dc.cleaning.rule.column.列名 value为：规则模板json中的位置
//      .option("dc.cleaning.rule.column.isCumulant", "1")
      // 规则服务提供的api,参数为规则模板code
      .option("dc.cleaning.rule.service.api", ruleServiceAPI)

    while (true) {
      val source = reader.load()
      val query = process(source).writeStream.outputMode("update")
        .foreachBatch(writeBatch _)
//        .format("console")
        .option("truncate", value = false)
        .option("checkpointLocation", checkpointLocation)
        .start()

      Thread.sleep(waitRestartTime)
      query.stop()
      query.awaitTermination(1000 * 60 * 5)
    }

    def process(source: DataFrame): DataFrame = {
      source
    }

    def writeBatch(source: DataFrame, batchId: Long): Unit = {
      //val dfCache = source.persist()

      val data1 = source.filter($"id" isNotNull)
      val r = RingRatio(data1,data1)

      val exceptionValues = r.filter($"isOK".equalTo(false)).drop("RingRatio","id","v2","isOK")
      val normalValues = r.filter($"isOK".equalTo(true)).drop("RingRatio","id","v2","isOK")
      val otherValues = source.filter($"id" isNull).withColumn("t",$"t".cast(LongType)).drop("id")

      exceptionWrite(exceptionValues, batchId)
      normalWrite(normalValues, batchId)
      otherWrite(otherValues, batchId)

      //dfCache.unpersist()
    }

    def normalWrite(normalValues: DataFrame, batchId: Long): Unit = {

      // TODO 正常数据写入Kafka
      println("--------normalWrite--------")
      normalValues.show(false)
    }

    def exceptionWrite(exceptionValues: DataFrame, batchId: Long): Unit = {
      println("--------exceptionWrite--------")
      // TODO 异常数据写入异常库
      exceptionValues.show(false)
    }

    def otherWrite(otherValues: DataFrame, batchId: Long): Unit = {
      println("--------otherWrite--------")
      // TODO 其他数据写入Kafka
      otherValues.show(false)

    }

    def RingRatio(df: DataFrame, df2: DataFrame): DataFrame = {
      //处理数据到
     // df.printSchema()

      val data1 =
        df
          .withColumn("t", $"t".cast(LongType))
//          .select($"v", $"t", $"uri")
          .withWatermark("t", "60 minutes") //设置水印
      val data2 = df2
        .withColumn("t2", $"t".cast(LongType))
        .withColumn("t1", ($"t".cast(LongType)+1))
        .select($"v".cast(DoubleType).as("v2"), $"t2", $"t1")
        .withWatermark("t2", "10 minutes") //设置水印
      //关联查询join
      data1
        .join(data2, data1("t") === data2("t1"), "rightouter")
        .drop("t1", "t2")
        .withColumn("RingRatio", round(($"v" - $"v2") / $"v2", 3))
        .withColumn("isOK", $"RingRatio"<=0.1 && $"RingRatio" >= -0.1)

    }
  }
}
