package com.owneriq.wh.processor.spark

import org.apache.spark.rdd.RDD
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.json.JsonArray
import com.owneriq.wh.model.campaign._
import com.owneriq.wh.model.auction.{Auction, Bid, Exchange}
import com.owneriq.wh.model.user.{Channel, Location, User}
import com.owneriq.wh.model.event.{UserCampaignEvent, UserCampaignEventGroup}
import com.owneriq.wh.transformer.UserCampaignTransformer

object AuctionMasterPivotProcessor {

  def processRDD(rdd : RDD[String]) : RDD[JsonDocument] = {

    val eventGroupRDD = rdd
      //.map(_.split("\t"))
      .map(buildUserCampaignEventGroup)
      .map( x => ((x.user.id, x.ali.id), x))
      .reduceByKey( (x, y) => x.merge(y) )
      .map(_._2)

    eventGroupRDD.map(buildJsonDocument)
  }

  val buildJsonDocument = (eventGroup: UserCampaignEventGroup) => {
    val user = eventGroup.user
    val ali = eventGroup.ali
    val eventList = eventGroup.eventList

//    val eventJsonArray = buildEventJsonArray(eventList)
    val eventJsonStringList = eventList.map(UserCampaignTransformer.transformJson)
    val eventJsonObjectList = eventJsonStringList.map(JsonObject.fromJson)

    val eventJsonArray = JsonArray.from(eventJsonObjectList)
//    val eventJsonArray = JsonArray.create()
//    eventJsonObjectList.foreach(eventJsonArray.add(_))

    val win_events = JsonObject.create()
      .put("idadvertiserlineitem", ali.id)
      .put("win_list", eventJsonArray)

    val key = "user_ali_win_event::" + user.id + "::" + ali.id

    JsonDocument.create(key,
      JsonObject.create()
        .put("user", user.id)
        .put("win_events", win_events))
  }

//  def buildEventJsonArrayOld(eventList: List[UserCampaignEvent]): JsonArray ={
//    val eventJsonArray = JsonArray.create()
//
//    for(userCampaignEvent <- eventList){
//      val win = userCampaignEvent.win
//      val eventJsonObject = JsonObject.create()
//        .put("idmostlineitem", userCampaignEvent.cli.id)
//        .put("isclick", win.isClick)
//        .put("adsize", win.auction.adSize)
//        .put("bid_request_data", win.requestData)
//        .put("utc_time", win.time)
//
//      eventJsonArray.add(eventJsonObject)
//    }
//
//    eventJsonArray
//  }
//
//  def buildEventJsonArray(eventList: List[UserCampaignEvent]): JsonArray ={
//    val eventJsonStringList = eventList.map(UserCampaignTransformer.transformJson)
//    val eventJsonObjectList = eventJsonStringList.map(JsonObject.fromJson)
//
//    val eventJsonArray = JsonArray.create()
//    eventJsonObjectList.foreach(eventJsonArray.add(_))
//
//    eventJsonArray
//  }

  val buildUserCampaignEventGroup = (line: String) => {
    val userCampaignEvent = UserCampaignTransformer.transform(line)
    new UserCampaignEventGroup(userCampaignEvent.user, userCampaignEvent.cli.owneriqTargetProfile.advertiserLineItem, List(userCampaignEvent))
  }

}
