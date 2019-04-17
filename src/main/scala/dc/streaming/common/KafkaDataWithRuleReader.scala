package dc.streaming.common

import com.alibaba.fastjson.parser.Feature
import com.alibaba.fastjson.{JSON, JSONObject, JSONPath}
import org.apache.http.client.fluent.Request
import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author : shirukai
  * @date : 2019-01-31 11:00
  */
class KafkaDataWithRuleReader(@transient spark: SparkSession) extends Serializable {

  import spark.implicits._
  import org.apache.spark.sql.functions._
  import KafkaDataWithRuleReader._

  private val extraOptions = new scala.collection.mutable.HashMap[String, String]


  private lazy val source: DataFrame = loadSource()
  private lazy val api: String = extraOptions(DC_CLEANING_RULE_SERVICE_API)
  private lazy val ruleOptions: collection.Map[String, String] =
    extraOptions.filterKeys(_.startsWith(DC_CLEANING_RULE_COLUMN_PREFIX))
      .map(x => x._1.stripPrefix(DC_CLEANING_RULE_COLUMN_PREFIX) -> x._2)


  def option(key: String, value: String): KafkaDataWithRuleReader = {
    this.extraOptions += (key -> value)
    this
  }

  private def handlerSource(source: DataFrame): DataFrame = {
    source.select(from_json('value.cast("string"), ArrayType(schema)).as("values"))
      .select(explode('values))
      .select("col.*")
      .withColumnRenamed("v", "value")
      .withColumnRenamed("t", "timestamp")
  }


  private def loadRule(): DataFrame = {
    import collection.JavaConverters._
    val rules = requestRules(api)
    val columnSchema = generateColumnSchema(ruleOptions.toMap)
    val values = rules.map(r => {
      val v = List(r._1, r._2("uri")) ::: ruleOptions.map(rc => {
        JSONPath.read(r._2("params"), rc._2).asInstanceOf[String]
      }).toList
      Row.fromSeq(v)
    }).toList.asJava
    spark.createDataFrame(values, columnSchema)
  }

  private def loadSource(): DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .options(extraOptions)
      .load()
    handlerSource(df).as("stream")
  }


  def load(): DataFrame = {
    val rulesDF = loadRule().as("rules")
    val df = source
      .filter(!_.getAs[String]("value").contains("S#"))
      // .withColumn("id", $"pointId")
      .join(rulesDF, expr(
      """
              rules.id = concat("/",stream.gatewayId,"/",stream.pointId)
            """
    ), "leftOuter")

    def handleValue(v: String): Double = {
      if (v.contains("#")) {
        val value = v.substring(2)
        if (v.contains("B#")) value match {
          case "true" => 1.0
          case _ => 0.0
        } else value.toDouble
      } else v.toDouble
    }

    val handleValueUDF = udf(handleValue _)
    df.withColumn("v", handleValueUDF($"value"))
      .withColumn("eventTime", ($"timestamp" / 1000).cast(TimestampType))
      .withColumn("t", $"timestamp".cast(LongType))
      .drop("value")

  }


}

object KafkaDataWithRuleReader {

  import org.apache.spark.sql.functions._

  val DC_CLEANING_RULE_COLUMN_PREFIX = "dc.cleaning.rule.column."
  val DC_CLEANING_RULE_SERVICE_API = "dc.cleaning.rule.service.api"

  private lazy val schema = StructType(Seq(
    StructField("gatewayId", StringType),
    StructField("namespace", StringType),
    StructField("pointId", StringType),
    //StructField("regions", StringType),
    StructField("t", StringType),
    StructField("v", StringType),
    StructField("s", StringType))
  )

  def generateColumnSchema(parameters: Map[String, String]): StructType = {
    StructType(List(StructField("id", StringType), StructField("uri", StringType)) ::: parameters.map(p => StructField(p._1, StringType)).toList)
  }

  def handleStreamDataFrame(stream: DataFrame): DataFrame = {
    stream.select(from_json(col("value").cast("string"), ArrayType(schema)).as("values"))
      .select(explode(col("values")))
      .select("col.*")
      .withColumnRenamed("v", "value")
      .withColumnRenamed("t", "timestamp")
  }

  def joinRules(stream: DataFrame, api: String, ruleOptions: Map[String, String]): DataFrame = {
    def handleValue(v: String): Double = {
      if (v.contains("#")) {
        val value = v.substring(2)
        if (v.contains("B#")) value match {
          case "true" => 1.0
          case _ => 0.0
        } else value.toDouble
      } else v.toDouble
    }

    val handleValueUDF = udf(handleValue _)
    val streamDataFrame = handleStreamDataFrame(stream).as("stream")
    val ruleDataFrame = loadRuleDataFrame(streamDataFrame.sparkSession, api, ruleOptions).as("rules")

    streamDataFrame.filter(!_.getAs[String]("value").contains("S#"))
      // .withColumn("id", $"pointId")
      .join(ruleDataFrame, expr(
      """
              rules.id = concat("/",stream.gatewayId,"/",stream.pointId)
            """
    ), "leftOuter")
      .withColumn("v", handleValueUDF(col("value")))
      .withColumn("eventTime", (col("timestamp") / 1000).cast(TimestampType))
      .withColumn("t", col("timestamp").cast(LongType))
      .drop("value")
  }

  def loadRuleDataFrame(@transient spark: SparkSession, api: String, ruleOptions: Map[String, String]): DataFrame = {
    val options = ruleOptions.filterKeys(_.startsWith(DC_CLEANING_RULE_COLUMN_PREFIX))
      .map(x => x._1.stripPrefix(DC_CLEANING_RULE_COLUMN_PREFIX) -> x._2)
    import collection.JavaConverters._
    val rules = requestRules(api)
    val columnSchema = generateColumnSchema(options)
    val values = rules.map(r => {
      val v = List(r._1, r._2("uri")) ::: options.map(rc => {
        JSONPath.read(r._2("params"), rc._2).asInstanceOf[String]
      }).toList
      Row.fromSeq(v)
    }).toList.asJava
    spark.createDataFrame(values, columnSchema)
  }

  def requestRules(api: String, retry: Int = 10): Map[String, Map[String, String]] = {
    var retries = 0
    var rules = Map[String, Map[String, String]]()
    import scala.collection.JavaConverters._
    while (retries < retry) {
      val response = Request.Get(api).execute().returnResponse()
      if (response.getStatusLine.getStatusCode == 200) {
        try {
          val res = JSON.parseObject(response.getEntity.getContent, classOf[JSONObject], Feature.OrderedField).asInstanceOf[JSONObject]
          rules = res.getJSONArray("data").asScala.flatMap(x => {
            val j = x.asInstanceOf[JSONObject]
            val instanceParams = j.getString("instanceParams")
            val pointIds = j.getJSONArray("points")
            pointIds.asScala.map(p => {
              val point = p.asInstanceOf[JSONObject]
              point.getString("id") -> Map("params" -> instanceParams, "uri" -> point.getString("uri"))
            })
          }).toMap
        } catch {
          case e: Exception => retries += 1
        }
        retries = retry
      }
      else retries += 1
    }
    rules
  }

  def main(args: Array[String]): Unit = {
    val ruleOptions = Map[String, String](
      "dc.cleaning.rule.column.upperbound" -> "$.0.value",
      "dc.cleaning.rule.column.lowerbound" -> "$.1.value"
    )
    val options = ruleOptions.filterKeys(_.startsWith(DC_CLEANING_RULE_COLUMN_PREFIX))
      .map(x => x._1.stripPrefix(DC_CLEANING_RULE_COLUMN_PREFIX) -> x._2)

    println(options)
  }
}

