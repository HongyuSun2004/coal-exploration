package com.owneriq.wh.processor.pixelEvent

import com.couchbase.spark._
import com.owneriq.wh.spark.couchbase.SparkConnector

object UserPixelEventBatchProcessor {

  def main(args: Array[String]) {

    val spark = SparkConnector.getSparkSession("UserPixelEventBatchProcessor", "","")
    val sc = spark.sparkContext

    val rdd = sc.textFile("/adnet_data/campaign_conversion_receipts_hourly/utc_date=2016-11-09/utc_hour=10/000109_0.snappy")

    CampaignConversionReceiptTransformer.transformRDD(rdd).saveToCouchbase()
  }

}
