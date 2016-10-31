package com.owneriq.wh.hdfs

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    //val logFile = "/Users/hsun/spark-2.0.1-bin-hadoop2.7/README.md" // Should be some file on your system

    val logFile = "/adnet_data/campaign_conversion_receipts_hourly/utc_date=2016-10-21/utc_hour=11/000003_0.snappy"

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
