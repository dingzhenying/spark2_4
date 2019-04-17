package dc.streaming.sink.kafkaSink

import java.util

import com.google.gson.Gson
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Dingzhenying on 2019/1/23
  *
  * kafkaSink写入示例
  */
object KafkaSinkWriterTest {
  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[4]")
    .getOrCreate()
  spark.conf.set("spark.sql.streaming.checkpointLocation", "./kafka" +
    "checkpointLocation") //设定
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //获取数据
    val df =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "192.168.66.194:9092")
        .option("subscribe", "dc-data")
        .option("failOnDataLoss", "false") //数据丢失f失败

        .load()
    //写kafka

    val CleanData = DataClean(df)
      .withColumn("day", to_date($"t", "yyyy-MM-dd HH:mm:ss"))

    val kafkaData = writerKafka(CleanData)
      .writeStream
      .outputMode("update")
      .format("demo.dataSink.DataStreamSinkProvider")
      .option("dataType", "kafka")
      .option("kafka.bootstrap.servers", "192.168.66.194:9092")
      .option("topic", "minuteDate")
      .start
    kafkaData.awaitTermination()
  }

  def DataClean(df: DataFrame): DataFrame = {
    //处理数据到
    val data = df.select($"value".cast(StringType))
      .as[(String)]
      .map(x => {
        //println(x)
        json2Object(x)
      }).flatMap(data => {
      data
    })
      .withColumn("type", $"v".substr(0, 1))
      .withColumn("v", splitData($"v", lit(2)))
      .withColumn("t", from_unixtime($"t" / 1000).cast(TimestampType))
      .groupBy(window($"t", "1 minute", "1 minute"), $"pointId") //滚动窗口前闭后开
      .agg(last("v").as("v"), last("t").as("t"))
    data
  }

  def writerKafka(df: DataFrame): DataFrame = {

    df.withColumn("key", $"pointId")
      .withColumn("value", jsonOne($"pointId", $"v", $"t"))

  }

  case class dataJson(namespace: String, internalSeriesId: String, regions: String, t: String, s: String, v: String, gatewayId: String, pointId: String)

  val splitData = udf((data: String, int: Int) => {
    data.substring(int)
  })

  val jsonOne = udf {
    (pointId: String, v: String, t: String) => {
      val gson = new Gson()
      var data = new util.HashMap[String, String]
      data.put("pointId", pointId)
      data.put("v", v)
      data.put("t", t)
      val dataJson = gson.toJson(data)
      dataJson
    }
  }

  def json2Object(json: String): Array[dataJson] = {
    val gson = new Gson()
    gson.fromJson(json, classOf[Array[dataJson]])
  }

}
