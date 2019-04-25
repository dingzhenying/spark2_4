package structuredStreaming

import com.google.gson.Gson
import com.spark.sparkSql.ReadTest.dataJson
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Dingzhenying on 2019/2/12
  */
object  foreachBeach {
  val url = "jdbc:postgresql://192.168.66.194:5432/dalong"
  val user = "postgres"
  val pwd = "postgres"
  val driver = "org.postgresql.Driver"
  val tableName="cleandata"
  private val URL = "localhost:2181"
  private val KAFKA_URL = "localhost:9092"
  private val NAME = "kafka_test"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").getOrCreate()
    import spark.implicits._
    val read=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_URL)
      .option("subscribe", NAME)
      .option("failOnDataLoss", "false") //数据丢失f失败
      //.option("startingOffsets", "latest")
      .load()
     Class.forName(driver)
     val writer= read
       .select(from_json($"value".cast(StringType),schema).as("value"))
       .select("value.*")
       .withColumn("t", from_unixtime($"t"/1000, "yyyy-MM-dd HH:mm:ss"))
       .withColumn("v", splitData($"v", lit(2)))
//       .groupBy(window($"t", "1 minute", "1 minute"), $"pointId") //滚动窗口前闭后开
//       .agg(avg("v").as("aveValue"),last("v").as("minValue"), last("t").as("time"))
       .writeStream
      .outputMode("update")
       .foreachBatch((batchDF: DataFrame, batchId: Long) =>
       {
         batchDF.persist()//缓存
         batchDF.show(false)
//          batchDF.write.format("jdbc").option("user",user).option("password",pwd).option("url",url).option("dbtable",tableName).mode(SaveMode.Append).save()
         // location 1
//          batchDF.write.format("jdbc").option("user",user).option("password",pwd).option("url",url).option("dbtable",tableName).mode(SaveMode.Append).save()
         // location 2
          batchDF.unpersist()//清除缓存
       }).start()
    writer.awaitTermination()
  }

  val splitData = udf((data: String, int: Int) => {
    data.substring(int)
  })

  def json2Object(json: String): Array[dataJson] = {
    val gson = new Gson()
    gson.fromJson(json, classOf[Array[dataJson]])
  }
  // Create scheme for value
  val schema = StructType(
    List(
    StructField("gatewayId", StringType),
    StructField("internalSeriesId", StringType),
    StructField("namespace", StringType),
    StructField("pointId", StringType),
    StructField("regions", StringType),
    StructField("t", StringType),
    StructField("v", StringType),
    StructField("s", StringType)
    )
  )
}
