/**
  * Created by hsun on 10/28/16.
  */

package com.owneriq.wh.processor

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object UserPixelEventProcessor {
  def main(args: Array[String]) {
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("UserPixelEventProcessor")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.

    val lines = ssc.textFileStream("/Users/hsun/spark-test")

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}