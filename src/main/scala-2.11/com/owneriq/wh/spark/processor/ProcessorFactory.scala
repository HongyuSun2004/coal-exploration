package com.owneriq.wh.spark.processor

object ProcessorFactory {
  def apply(name: String): ProcessorTrait =
    name match {
      case "UserCampaignProcessor" => UserCampaignProcessor
      case "UserPixelProcessor" => UserPixelProcessor
      case _ => throw new IllegalArgumentException(name + " is not a supported processor")
    }
}
