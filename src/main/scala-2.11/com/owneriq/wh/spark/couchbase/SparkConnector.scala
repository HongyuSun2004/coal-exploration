package com.owneriq.wh.spark.couchbase

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.spark.SparkContextFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * The SparkConnector connects spark program to Couchbase cluster
  */
object SparkConnector {
  val couchbaseNodes = "ec2-52-42-137-48.us-west-2.compute.amazonaws.com;ec2-52-39-179-8.us-west-2.compute.amazonaws.com;ec2-52-36-239-88.us-west-2.compute.amazonaws.com"

  /**
    * Create a spark session with Couchbase nodes and bucket config
    * @param appName Application Name
    * @param bucketName Couchbase Bucket Name
    * @param bucketPassword Couchbase Bucket password
    * @return Spark Session
    */
  def getSparkSession(appName: String, bucketName: String, bucketPassword: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.couchbase.nodes", couchbaseNodes) // connect to couchbase server
      .config("spark.couchbase.bucket." + bucketName, bucketPassword) // open the whtest bucket with password
      .getOrCreate()
  }

  def getSparkSession(processName: String): SparkSession =
    processName match {
      case "UserCampaignProcessor" => getSparkSession(processName, "whtest2", "whtest2")
      case _ => throw new IllegalArgumentException(processName + " is not supported")
    }


  /**
    * Create a spark session with Couchbase user campaign bucket config
    * @param appName Application Name
    * @return Spark Session
    */
  def getUserAliWinSparkSession(appName: String): SparkSession = {
    //getSparkSession(appName, "wh-winevent", "")
    getSparkSession(appName, "whtest2", "whtest2")
  }

  /**
    * Retrieve a json document from couchbase
    * @param sc spark context
    * @param key couchbase json document key
    * @return RDD of json document
    */
  def getRDD(sc: SparkContextFunctions, key: String): RDD[JsonDocument]={
    sc.couchbaseGet[JsonDocument](Seq(key))
  }
}
