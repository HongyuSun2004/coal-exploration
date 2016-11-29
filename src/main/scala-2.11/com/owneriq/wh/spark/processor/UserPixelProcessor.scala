package com.owneriq.wh.spark.processor

import com.couchbase.client.java.document.JsonDocument
import com.owneriq.wh.model.event.UserPixelEvent
import com.owneriq.wh.transformer.UserPixelTransformer
import org.apache.spark.rdd.RDD

object UserPixelProcessor extends ProcessorTrait{
  override def transformRDD(rdd : RDD[String]) : RDD[JsonDocument] = {
    rdd.map(UserPixelTransformer.transform)
      .map(UserPixelTransformer.transformJson)

  }

  /**
    * The function of build user campaign event model object from the flat string
    */
  val buildUserCampaignEventGroup: (String) => UserPixelEvent = (line: String) => {
    UserPixelTransformer.transform(line)
  }
}
