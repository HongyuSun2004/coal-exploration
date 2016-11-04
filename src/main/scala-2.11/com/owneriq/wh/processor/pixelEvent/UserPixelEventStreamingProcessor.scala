package com.owneriq.wh.processor.pixelEvent

import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.couchbase.spark._

object UserPixelEventStreamingProcessor {
  def main(args: Array[String]) {

    val spark = UserPixelEventProcessor.getSparkSession("UserPixelEventStreamingProcessor")
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create a HDFS stream on campaign_conversion_receipts
    //val lines = ssc.textFileStream("/Users/hsun/spark-test/streaming/")
    val lines = ssc.textFileStream("/user/hsun/test_data/campaign_conversion_receipts/")

    lines.foreachRDD(rdd => UserPixelEventProcessor.transformRDD(rdd).saveToCouchbase())

    ssc.start()
    ssc.awaitTermination()
  }
}
