package com.owneriq.wh.spark.processor

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.spark.SparkContextFunctions
import org.apache.spark.rdd.RDD

trait ProcessorTrait {
  //def loadRDD(sc: SparkContextFunctions, source: String): RDD[String]

  def processRDD(sc: SparkContextFunctions, rdd : RDD[String]): RDD[JsonDocument]

  //def saveRDD(sc: SparkContextFunctions, rdd : RDD[T]): Unit
}
