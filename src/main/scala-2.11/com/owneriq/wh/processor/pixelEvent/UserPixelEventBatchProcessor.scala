package com.owneriq.wh.processor.pixelEvent

import com.couchbase.spark._

object UserPixelEventBatchProcessor {

  def main(args: Array[String]) {

    val spark = UserPixelEventProcessor.getSparkSession("UserPixelEventBatchProcessor")
    val sc = spark.sparkContext

    //val rdd = sc.textFile("/adnet_data/campaign_conversion_receipts_hourly/utc_date=2016-11-02/*/")
    val rdd = sc.textFile("/user/hsun/test_data/campaign_conversion_receipts/*/")
    //val rdd = sc.textFile("/Users/hsun/spark-test/*/")

    UserPixelEventProcessor.transformRDD(rdd).saveToCouchbase()
  }

}
