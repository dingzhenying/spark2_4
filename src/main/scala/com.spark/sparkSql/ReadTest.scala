package com.spark.sparkSql

import com.google.gson.Gson
import structuredStreaming.foreachBeach.schema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/**
  * Created by Dingzhenying on 2019/2/12
  */
object ReadTest {
  val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val readata=spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.66.194:9092")
      .option("subscribe", "dc-data")
      .option("failOnDataLoss", "false")
      .load()

    readata.printSchema()
    val writer=DataClean(readata)
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .start()
    writer.awaitTermination()
    println("ddddd")

  }

  case class cleanDataJson(time: String, node_uri: String, factory_uri: String, value: Double, abnormal_type: String, upper_bound: Double, lower_bound: Double, previous_value: Double, state: String)

  def DataClean(df: DataFrame): DataFrame = {
    //处理数据到
    val data = df
      .select(from_json($"value".cast(StringType),schema).as("value"))
      .select("value.*")
      .withColumn("type", $"v".substr(0, 1))
      .filter($"type" === "F")
      .withColumn("v", splitData($"v", lit(2)))
      .withColumn("t", from_unixtime($"t" / 1000).cast(TimestampType))
      .groupBy(window($"t", "1 minute", "2 seconds"), $"pointId", $"namespace") //滚动窗口前闭后开
      .agg(last("v").as("v"), last("t").as("t"))
    data
  }
  val splitData = udf((data: String, int: Int) => {
    data.substring(int)
  })
  val limitDiscriminateUDF = udf(simpleDiscriminate _)

  def simpleDiscriminate(v: Double, lower_bound: Double, upper_bound: Double): Boolean = {
    lower_bound < v && v < upper_bound
  }

  case class dataJson(namespace: String, internalSeriesId: String, regions: String, t: String, s: String, v: String, gatewayId: String, pointId: String)

  def json2Object(json: String): Array[dataJson] = {
    val gson = new Gson()
    gson.fromJson(json, classOf[Array[dataJson]])
  }

  // Create scheme for value
  val schema = StructType(List(
    StructField("gatewayId", StringType),
    StructField("internalSeriesId", StringType),
    StructField("namespace", StringType),
    StructField("pointId", StringType),
    StructField("regions", StringType),
    StructField("t", StringType),
    StructField("v", StringType),
    StructField("s", StringType))
  )
}
