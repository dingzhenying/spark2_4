package dc.streaming.common

import java.sql.Timestamp
import java.util

import dc.common.hbase.HBaseUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col

/**
  * Created by shirukai on 2019-03-13 11:
  * foreach batch 写入HBase
  */
object HBaseDataBatchWriter {
  def save(df: DataFrame,
           batchId: Long,
           hbaseZookeeperQuorum: String,
           hbaseRegionNumber: Int,
           hbaseTableName: String): Unit = {
    val data = formatHBaseData(df, hbaseRegionNumber)
    data.foreachPartition(i => {
      val puts = new util.ArrayList[Put]()
      i.foreach(r => {
        val columnFamaliyName: String = "c"
        val rowkey = r.getAs[String]("rowkey")
        val put = new Put(Bytes.toBytes(rowkey))
        val t = r.getAs[Timestamp]("t").getTime
        val v = r.getAs[Double]("v")
        put.addColumn(
          Bytes.toBytes(columnFamaliyName),
          Bytes.toBytes("v"),
          Bytes.toBytes(v)
        )
        //todo 数据进入清洗系统的时间
        put.addColumn(
          Bytes.toBytes(columnFamaliyName),
          Bytes.toBytes("t"),
          Bytes.toBytes(t)
        )
        puts.add(put)
      })
      HBaseUtil.putRows(hbaseZookeeperQuorum, hbaseTableName, puts)
    })
  }

  def formatHBaseData(df: DataFrame, hbaseRegionNumber: Int): DataFrame = {
    val generateRowKey: UserDefinedFunction = udf {
      (uri: String, t: Timestamp) => {
        val numRegions = hbaseRegionNumber.toInt
        val timestamp = t.getTime

       /* val salt = Math.abs(uri.hashCode) % numRegions
        var row = "0"
        if (salt < 10) row = "0" + salt + "/" + uri + timestamp //补0
        else row = salt + "/" + uri + timestamp
        row.substring(0, row.length) //去掉毫秒*/
        HBaseUtil.getHashRowkeyWithSalt(uri,timestamp,numRegions)
      }
    }
    df.withColumn("rowkey", generateRowKey(col("uri"), col("t")))
  }


}
