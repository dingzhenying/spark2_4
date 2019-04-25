package structuredStreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by Dingzhenying on 2019/4/25
  */
object kafakReadUntil {
  def main(args: Array[ String ]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    import spark.implicits._

      val kafkaData = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "microservice-dc-1:9092")
        .option("subscribe", "test_dis-upper-lower-limits")
        .option("failOnDataLoss", "false") //数据丢失f失败
        //.option("startingOffsets", "latest")
        .load()
      val data = kafkaData
        //.select(schema_of_json($"value"))
        .select(from_json($"value".cast(StringType), schema).as("value"))
        .select($"value.*")
      val write = data
        .writeStream
        .outputMode("update")
//        .foreachBatch((batchDF: DataFrame, batchId: Long) =>{
//          batchDF.printSchema()
//          batchDF.show(100000,false)
//        })
        .format("console")
        .option("truncate", value = false)
//        .option("numRows", value = 100000)
        .start()
      write.awaitTermination()
      write.stop()
  }
    //  分钟取值
    val schema = StructType(
      List(
        StructField("gatewayId", StringType),
        StructField("namespace", StringType),
        StructField("pointId", StringType),
        StructField("t", StringType),
        StructField("v", StringType),
        StructField("s", StringType)
      )
    )
}
