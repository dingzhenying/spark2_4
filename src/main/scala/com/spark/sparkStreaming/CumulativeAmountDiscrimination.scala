package com.spark.sparkStreaming

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Dingzhenying on 2019/2/18
  *
  * 通过lag查询挡墙数据的上一条数据(错位查询)
  *
  */
object CumulativeAmountDiscrimination {
  val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.codegen.wholeStage", false)
  spark.conf.set("spark.debug.maxToStringFields", 1000)

  def main(args: Array[String]): Unit = {

    val read = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.66.194:9092")
      .option("subscribe", "dc-data")
      .option("failOnDataLoss", "false") //数据丢失f失败
      //.option("startingOffsets", "latest")
      .load()

    val data = read
      .select(from_json($"value".cast(StringType), schema).as("value"))
      .select("value.*")
      //      .filter($"pointId".equals("000002F"))
      .withColumn("t", from_unixtime($"t" / 1000, "yyyy-MM-dd HH:mm:ss"))
      .withColumn("v", splitData($"v", lit(2)))
      .groupBy(window($"t", "1 minute", "1 second"), $"pointId") //滚动窗口前闭后开
      .agg(last("v").as("minValue"), last("t").as("time"))
    val writer = data
      .writeStream
      .outputMode("update")
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        val df = batchDF.persist()
        //缓存
        //创建窗口
        val data = Window.partitionBy("pointId")
          .orderBy("window")
        val options = Map("user" -> "postgres", "password" -> "postgres", "dbtable" -> "tasttable", "url" -> "jdbc:postgresql://192.168.66.194:5432/dalong")
        val data2 = df.withColumn("lastdata", lag($"minValue", 1).over(data)).withColumn("window", $"window.start")
        data2.printSchema()
        data2.write.format("jdbc").options(options).mode(SaveMode.Overwrite).save()
        data2.show(false)
        //batchDF.write.format("jdbc").option("user",user).option("password",pwd).option("url",url).option("dbtable",tableName).mode(SaveMode.Append).save()
        // location 1
        //batchDF.write.format("jdbc").option("user",user).option("password",pwd).option("url",url).option("dbtable",tableName).mode(SaveMode.Append).save()
        // location 2
        df.unpersist() //清除缓存
      })
      //.format("console")
      //.option("truncate",value =false)
      .start()
    writer.awaitTermination()
  }

  val splitData = udf((data: String, int: Int) => {
    data.substring(int)
  })


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
