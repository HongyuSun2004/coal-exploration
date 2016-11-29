package com.owneriq.wh.spark.driver

import com.couchbase.spark._
import com.owneriq.wh.spark.couchbase.SparkConnector
import com.owneriq.wh.spark.processor.ProcessorFactory

object BatchProcessor {
  def main(args: Array[String]): Unit = {
    val processorName = args(0)
    val sourcePath = args(1)

    val spark = SparkConnector.getSparkSession(processorName)
    val processor = ProcessorFactory(processorName)

    val sc = spark.sparkContext
    val rdd = sc.textFile(sourcePath)

    processor.processRDD(sc, rdd)
      .saveToCouchbase()
  }
}
