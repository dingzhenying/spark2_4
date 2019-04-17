package dc.streaming.sink

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.hbase.HBaseSink
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.kafka010.{JdbcDBSink, KafkaNewSink}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode


/**
  * Created by Dingzhenying on 2019/1/22
  *
  * DataSinkProvider (总的Sink调度Provider)
  */

class DataStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    if (parameters.get("dataType").get=="mysql"){
      println("jdbcWriter start......")
      val defaultTopic = parameters.get("topic").map(_.trim)
      new JdbcDBSink(sqlContext,parameters, outputMode)
    }else if (parameters.get("dataType").get=="kafka"){
      println("kafkaWriter start......")
      val defaultTopic = parameters.get("topic").map(_.trim)
      new KafkaNewSink(sqlContext, parameters, defaultTopic)
    }else if (parameters.get("dataType").get=="hbase"){
      println("hbase start......")
      new HBaseSink(parameters)
    }else{
      println("otherWriter start......")
      new JdbcDBSink(sqlContext,parameters, outputMode)
    }
  }
  //此名称可以在.format中使用。
  override def shortName(): String = "dataSink"
}
