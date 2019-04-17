package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class HBaseSink(options: Map[String, String]) extends Sink with Logging {
  // String with HBaseTableCatalog.tableCatalog
  private val hBaseCatalog = options.get("hbasecat").map(_.toString).getOrElse("")

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {

    val query = data.queryExecution
    val rdd = query.toRdd
    val df=data.sqlContext.internalCreateDataFrame(rdd, data.schema)
    //val df = data.sparkSession.createDataFrame(rdd, data.schema)
    df.write
      .options(Map(HBaseTableCatalog.tableCatalog->hBaseCatalog,
        HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}