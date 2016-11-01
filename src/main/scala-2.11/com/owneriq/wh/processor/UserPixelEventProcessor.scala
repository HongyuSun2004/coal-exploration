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

    // Create the context with a 2 second batch size
    val ssc = new StreamingContext(sc, Seconds(2))

    // Create a HDFS stream on campaign_conversion_receipts
    //val lines = ssc.textFileStream("/Users/hsun/spark-test/streaming/")
    val lines = ssc.textFileStream("/user/hsun/test_data/campaign_conversion_receipts/")

    lines.foreachRDD(rdd => {
      rdd.filter(_.trim().length > 0).map(_.split("\t"))
        .map(p => convert(p)  )
        .map(ccp => JsonDocument.create(
          "userPixelEvent::" + ccp.sadnetuuid,
          JsonObject.create()
          .put("sadnetuuid", ccp.sadnetuuid)
          .put("idadvertiserlineitem", ccp.idadvertiserlineitem))
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
    var pagestamp = ""

    if (p.length == 11)
      pagestamp = p(10).trim

    p.foreach(println)

    CampaignConversionReceipt(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt, p(4).trim, p(5).toInt, BigInt(p(6)), p(7).trim, p(8).toBoolean, p(9).toInt , pagestamp)
  }

}