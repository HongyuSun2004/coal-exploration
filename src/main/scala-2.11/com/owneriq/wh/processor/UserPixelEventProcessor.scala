/**
  * Created by hsun on 10/28/16.
  */

package com.owneriq.wh.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark._

object UserPixelEventProcessor {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("UserPixelEventProcessor")
      .config("spark.couchbase.nodes", "ec2-52-42-137-48.us-west-2.compute.amazonaws.com;ec2-52-39-179-8.us-west-2.compute.amazonaws.com;ec2-52-36-239-88.us-west-2.compute.amazonaws.com") // connect to couchbase server
      .config("spark.couchbase.bucket.whtest", "whtest") // open the whtest bucket with password
      .getOrCreate()

    // The SparkContext for easy access
    val sc = spark.sparkContext

    // Create the context with a 5 second batch size
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create a HDFS stream on campaign_conversion_receipts
    //val lines = ssc.textFileStream("/Users/hsun/spark-test/streaming/")
    val lines = ssc.textFileStream("/user/hsun/test_data/campaign_conversion_receipts/")

    lines.foreachRDD(rdd => {
      rdd.filter(_.trim().length > 0)
        .map(_.split("\t"))
        .filter(_.length >= 10)
        .map(p => convert(p))
        .map(receipt =>
          JsonDocument.create("userPixelEvent::" + receipt.sadnetuuid,
            JsonObject.create()
              .put("sadnetuuid", receipt.sadnetuuid)
              .put("idadvertiserlineitem", receipt.idadvertiserlineitem)
              .put("order_datestart", receipt.order_datestart)
              .put("order_dateend", receipt.order_dateend)
              .put("click_conversion_lookback", receipt.click_conversion_lookback)
              .put("conversion_timestamp", receipt.conversion_timestamp)
              .put("idconversionsegment", receipt.idconversionsegment.toString)
              .put("conversion_type", receipt.conversion_type)
              .put("isoptimized", receipt.isoptimized)
              .put("view_conversion_lookback", receipt.view_conversion_lookback)
              .put("pagestamp", receipt.pagestamp)
          )
        )
        .saveToCouchbase()
    })

    ssc.start()
    ssc.awaitTermination()
  }

  case class CampaignConversionReceipt(idadvertiserlineitem: Int,
                    order_datestart: String,
                    order_dateend: String,
                    click_conversion_lookback: Int,
                    sadnetuuid: String,
                    conversion_timestamp: Int,
                    idconversionsegment: BigInt,
                    conversion_type: String,
                    isoptimized: Boolean,
                    view_conversion_lookback: Int,
                    pagestamp: String)

  def convert(p: Array[String]) : CampaignConversionReceipt = {

    def getPageStamp() = if (p.length == 11) p(10).trim else ""

    CampaignConversionReceipt(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt, p(4).trim, p(5).toInt, BigInt(p(6)), p(7).trim, p(8).toBoolean, p(9).toInt, getPageStamp)
  }

}