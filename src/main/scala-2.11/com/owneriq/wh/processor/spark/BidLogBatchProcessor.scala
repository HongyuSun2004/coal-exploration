package com.owneriq.wh.processor.spark

import com.couchbase.spark._
import com.owneriq.wh.couchbase.SparkConnector

object BidLogBatchProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkConnector.getUserAliWinSparkSession("BidLogUserBatchProcessor")
    val sc = spark.sparkContext

    val rdd = sc.textFile("/Users/hsun/spark-test/part-00000")

    AuctionMasterPivotProcessor.processRDD(rdd).saveToCouchbase()

  }
}
