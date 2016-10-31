/**
  * Created by hsun on 10/28/16.
  */
package com.owneriq.wh.couchbase

import org.apache.spark.sql.SparkSession
import com.couchbase.spark._
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject

object Quickstart {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("KeyValueExample")
      .master("local[*]") // use the JVM as the master, great for testing
      .config("spark.couchbase.nodes", "ec2-52-42-137-48.us-west-2.compute.amazonaws.com;ec2-52-39-179-8.us-west-2.compute.amazonaws.com;ec2-52-36-239-88.us-west-2.compute.amazonaws.com") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.whtest", "whtest") // open the travel-sample bucket with empty password
      .getOrCreate()
//bucket = Bucket('couchbase://ec2-52-42-137-48.us-west-2.compute.amazonaws.com;ec2-52-39-179-8.us-west-2.compute.amazonaws.com;ec2-52-36-239-88.us-west-2.compute.amazonaws.com:8091/python-tapad')

    // The SparkContext for easy access
    val sc = spark.sparkContext

    sc.couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748"))
      .collect()
      .foreach(println)

    sc.couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748"))
      .map(oldDoc => {
        val id = "my_" + oldDoc.id()
        val content = JsonObject.create().put("name", oldDoc.content().getString("name"))
        JsonDocument.create(id, content)
      })
      .saveToCouchbase()
  }

}