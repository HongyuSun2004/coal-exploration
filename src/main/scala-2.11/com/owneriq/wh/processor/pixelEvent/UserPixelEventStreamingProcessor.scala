package com.owneriq.wh.processor.pixelEvent

import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.couchbase.spark._

import com.owneriq.wh.couchbase.SparkConnector

object UserPixelEventStreamingProcessor {
  def main(args: Array[String]) {

    val spark = SparkConnector.getSparkSession("UserPixelEventStreamingProcessor")
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(6))

    // Create a HDFS stream on campaign_conversion_receipts
    val fileStream = ssc.textFileStream("/user/hsun/test_data/campaign_conversion_receipts/")

    fileStream.foreachRDD(rdd => CampaignConversionReceiptTransformer.transformRDD(rdd).saveToCouchbase())

    ssc.start()
    ssc.awaitTermination()
  }
}

