package org.apache.spark.sql.kafka010

import java.{util => ju}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.kafka010.KafkaSourceProvider.kafkaParamsForProducer
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.JavaConverters._

/**
  * Created by Dingzhenying on 2019/1/24
  *
  * 公用的kafkaSink
  */
class KafkaNewSink(sqlContext: SQLContext,
                   parameters: Map[String, String],
                   topic: Option[String]) extends Sink with Logging {
  @volatile private var latestBatchId = -1L

  override def toString(): String = "KafkaSink2"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val specifiedKafkaParams = kafkaParamsForProducer(parameters)
      val executorKafkaParams=new ju.HashMap[String, Object](specifiedKafkaParams.asJava)
      KafkaWriter.write(sqlContext.sparkSession,
        data.queryExecution, executorKafkaParams, topic)
      latestBatchId = batchId
    }
  }
}