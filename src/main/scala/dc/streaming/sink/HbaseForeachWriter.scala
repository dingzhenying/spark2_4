package dc.streaming.sink

import java.util.concurrent.ExecutorService

import dc.common.hbase.HBaseUtil
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.security.User
import org.apache.spark.sql.ForeachWriter


trait HBaseForeachWriter[RECORD] extends ForeachWriter[RECORD] {

  val hbase_zookeeper_quorum:String
  val tableName: String
  val hbaseConfResources: Seq[String]

  def pool: Option[ExecutorService] = None
  def user: Option[User] = None

  private var hTable: Table = _
  private var connection: Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    hTable = HBaseUtil.getTable(hbase_zookeeper_quorum,tableName)
    true
  }

  override def process(record: RECORD): Unit = {
    //HBaseUtil.putRow(tableName, record.toString(),"info","info",record.toString())
    val put = toPut(record)
    hTable.put(put)
  }

  override def close(errorOrNull: Throwable): Unit = {
    hTable.close()
    //HBaseUtil.
    //connection.close()
  }

  def toPut(record: RECORD): Put

}
