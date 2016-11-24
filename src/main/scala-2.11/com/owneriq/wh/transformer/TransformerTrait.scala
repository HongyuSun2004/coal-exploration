package com.owneriq.wh.transformer

trait TransformerTrait[T]{
  def transform(line: String): T
//  def transformJson(o: T): String
//  def transformJson(jsonString: String): T

  import net.liftweb.json._
  import net.liftweb.json.Serialization.write
  implicit val formats = DefaultFormats

  def transformJson(userCampaignEvent: T): String = {
    val ser = write(userCampaignEvent)
    ser.toString
  }

  def transformJson(jsonString: String): T ={
    val jValue = parse(jsonString)
    jValue.extract[T]
  }
}



