package com.owneriq.wh.couchbase

import org.apache.spark.sql.SparkSession

object SparkConnector {
  val couchbaseNodes = "ec2-52-42-137-48.us-west-2.compute.amazonaws.com;ec2-52-39-179-8.us-west-2.compute.amazonaws.com;ec2-52-36-239-88.us-west-2.compute.amazonaws.com"

  def getSparkSession(appName: String, bucketName: String, bucketPassword: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.couchbase.nodes", couchbaseNodes) // connect to couchbase server
      .config("spark.couchbase.bucket." + bucketName, bucketPassword) // open the whtest bucket with password
      .getOrCreate()
  }

  def getUserAliWinSparkSession(appName: String): SparkSession = {
    //getSparkSession(appName, "wh-winevent", "")
    getSparkSession(appName, "whtest2", "whtest2")
  }
}
