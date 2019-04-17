package dc.streaming.sink.jdbcSink

import java.sql.{Connection, Statement}

import com.google.gson.Gson
import demo.SwitchDataCleaning.splitData
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}


/**
  * Created by Dingzhenying on 2018/9/49:38
  * JDBC sink写入示例
  */
object JdbcSinkWriterTest {
  val spark = SparkSession.builder()
    .master("local[2]")
    .config("spark.debug.maxToStringFields", 100)
    .appName("StructuredStreamingKafkaMysql")
    .getOrCreate()

  import spark.implicits._

  val url = "jdbc:postgresql://192.168.66.194:5432/dalong"
  val user = "postgres"
  val pwd = "postgres"
  val driver = "org.postgresql.Driver"
  val tableName = "abnormaldata"
  var statement: Statement = _
  var connection: Connection = _

  def main(args: Array[String]): Unit = {

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.66.194:9092")
      .option("subscribe", "dc-data")
      .option("failOnDataLoss", "false") //数据丢失f失败
      .load()
    spark.conf.set("spark.sql.streaming.checkpointLocation", "./kafkaCheckpointLocation") //设定

    val limitDF = Seq(("000000", 0, 1548122)).toDF("namespace", "upper_bound", "lower_bound")

    val lines = DataClean(df)
      .join(limitDF, "namespace")
      .select($"*", limitDiscriminateUDF($"v", $"lower_bound", $"upper_bound").as("state")) //上下限判断
      .withColumnRenamed("t","time")
      .withColumnRenamed("id","node_uri")
      .withColumnRenamed("namespace","factory_uri")
      .withColumnRenamed("v","value")
      .withColumnRenamed("lowerbound","lower_bound")
      .withColumnRenamed("upperbound","upper_bound")
      .withColumn("time",from_unixtime($"time" / 1000).cast(TimestampType))
      .drop("window")
    lines.printSchema()
    //输出数据到异常库
    val query = lines
          .writeStream
          .outputMode("update")
          .format("demo.dataSink.DataStreamSinkProvider")
          .option("dataType", "mysql")
          .option("user", user)
          .option("password", pwd)
          .option("dbtable", tableName)
          .option("url", url)
          .start
    query.awaitTermination()

  }

  case class cleanDataJson(time: String, node_uri: String, factory_uri: String, value: Double, abnormal_type: String, upper_bound: Double, lower_bound: Double, previous_value: Double, state: String)

  def DataClean(df: DataFrame): DataFrame = {
    //处理数据到
    val data = df.selectExpr("CAST(value as STRING)") //对字段进行UDF操作，并返回该列
      .as[String]
      .map(x => {
        json2Object(x)
      })
      .flatMap(data => {
        data
      })
      .withColumn("type", $"v".substr(0, 1))
      .filter($"type" === "F")
      .withColumn("v", splitData($"v", lit(2)))
      .withColumn("t", from_unixtime($"t" / 1000).cast(TimestampType))
      .groupBy(window($"t", "1 minute", "2 seconds"), $"pointId", $"namespace") //滚动窗口前闭后开
      .agg(last("v").as("v"), last("t").as("t"))
    data
  }

  val limitDiscriminateUDF = udf(simpleDiscriminate _)

  def simpleDiscriminate(v: Double, lower_bound: Double, upper_bound: Double): Boolean = {
    lower_bound < v && v < upper_bound
  }

  case class dataJson(namespace: String, internalSeriesId: String, regions: String, t: String, s: String, v: String, gatewayId: String, pointId: String)

  def json2Object(json: String): Array[dataJson] = {
    val gson = new Gson()
    gson.fromJson(json, classOf[Array[dataJson]])
  }

}
