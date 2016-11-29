package com.owneriq.wh.processor.pixelEvent

import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.couchbase.spark.streaming._
import com.owneriq.wh.spark.couchbase.SparkConnector

object CBEventStreamingProcessor {

    def main(args: Array[String]) {

      val spark = SparkConnector.getSparkSession("CBEventStreamingProcessor", "","")
      val sc = spark.sparkContext
      val ssc = new StreamingContext(sc, Seconds(30))

      ssc.couchbaseStream(from = FromNow, to = ToInfinity)
        .filter(_.isInstanceOf[Mutation])
        .map(m => new String(m.asInstanceOf[Mutation].content))
        .saveAsTextFiles("/user/hsun/test_data/couchbase/test","")

      ssc.start()
      ssc.awaitTermination()
    }

}
