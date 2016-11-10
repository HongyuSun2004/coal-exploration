package com.owneriq.wh.couchbase

import org.apache.spark.sql.SparkSession

object SparkConnector {
  def getSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.couchbase.nodes", "ec2-52-42-137-48.us-west-2.compute.amazonaws.com;ec2-52-39-179-8.us-west-2.compute.amazonaws.com;ec2-52-36-239-88.us-west-2.compute.amazonaws.com") // connect to couchbase server
      .config("spark.couchbase.bucket.whtest2", "whtest2") // open the whtest bucket with password
      .getOrCreate()
  }
}
