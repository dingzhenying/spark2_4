package org.apache.spark.sql.kafka010

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by Dingzhenying on 2019/1/22
  *
  * JDBCSink(基于JDBC的数据连接)
  */
class JdbcDBSink(sqlContext: SQLContext, options: Map[String, String], outputMode: OutputMode) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    //流式datafream转RDD写入数据库
    sqlContext.internalCreateDataFrame(data.queryExecution.toRdd, data.schema)
      .write.format("jdbc").options(options).mode(SaveMode.Append).save()
  }
}
