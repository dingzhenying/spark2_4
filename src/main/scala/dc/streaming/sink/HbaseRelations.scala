package org.apache.spark.sql.execution.datasources.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by Dingzhenying on 2019/3/16
  */
case class HbaseRelations (
                            parameters: Map[String, String],
                            userSpecifiedschema: Option[StructType]
                          )(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with Logging {

  // insert
  // unhandledFilters
  // buildScan
  //配置信息
  val catalog = HBaseTableCatalog(parameters)
  val conf = HBaseConfiguration.create
  val serializedToken = SHCCredentialsManager.manager.getTokenForCluster(conf)

  //RDD_SCHEMA
  override val schema: StructType = userSpecifiedschema.getOrElse(catalog.toDataType)
  //数据过滤
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = ???
  val put= new Put("测试put".getBytes())
  def convertToPut(row: Row) = {
//    row
    (new ImmutableBytesWritable, put)
  }
  //数据写入
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    data.rdd.mapPartitions(iter => {
      SHCCredentialsManager.processShcToken(serializedToken)
      iter.map(convertToPut)
    }).saveAsNewAPIHadoopDataset(conf)

  }

}