package com.owneriq.wh.spark.driver

import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.couchbase.spark._
import com.owneriq.wh.spark.couchbase.SparkConnector
import com.owneriq.wh.spark.processor.ProcessorFactory

object StreamingProcessor {
  def main(args: Array[String]) {
    val processorName = args(0)
    val sourcePath = args(1)

    val spark = SparkConnector.getSparkSession(processorName)
    val processor = ProcessorFactory(processorName)
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(6))

    val fileStream = ssc.textFileStream(sourcePath)

    fileStream.foreachRDD(rdd => processor.processRDD(sc, rdd).saveToCouchbase())

    ssc.start()
    ssc.awaitTermination()
  }
}
