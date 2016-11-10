package com.owneriq.wh.processor.pixelEvent

import org.apache.spark.rdd.RDD
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject

object CampaignConversionReceiptTransformer {

  def transformRDD(rdd : RDD[String]) : RDD[JsonDocument] = {
    rdd.filter(_.trim().length > 0)
      .map(_.split("\t"))
      .filter(_.length >= 10)
      .map(p => convert(p))
      .map(receipt =>
        JsonDocument.create("userPixelEvent::" + receipt.sadnetuuid,
          JsonObject.create()
            .put("sadnetuuid", receipt.sadnetuuid)
            .put("idadvertiserlineitem", receipt.idadvertiserlineitem)
            .put("order_datestart", receipt.order_datestart)
            .put("order_dateend", receipt.order_dateend)
            .put("click_conversion_lookback", receipt.click_conversion_lookback)
            .put("conversion_timestamp", receipt.conversion_timestamp)
            .put("idconversionsegment", receipt.idconversionsegment.toString)
            .put("conversion_type", receipt.conversion_type)
            .put("isoptimized", receipt.isoptimized)
            .put("view_conversion_lookback", receipt.view_conversion_lookback)
            .put("pagestamp", receipt.pagestamp)
        )
      )
  }

  case class CampaignConversionReceipt(idadvertiserlineitem: Int,
                                       order_datestart: String,
                                       order_dateend: String,
                                       click_conversion_lookback: Int,
                                       sadnetuuid: String,
                                       conversion_timestamp: Int,
                                       idconversionsegment: BigInt,
                                       conversion_type: String,
                                       isoptimized: Boolean,
                                       view_conversion_lookback: Int,
                                       pagestamp: String)

  def convert(p: Array[String]) : CampaignConversionReceipt = {
    val pagestamp = if (p.length == 11) p(10).trim else ""

    CampaignConversionReceipt(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt, p(4).trim, p(5).toInt, BigInt(p(6)), p(7).trim, p(8).toBoolean, p(9).toInt , pagestamp)
  }

}
