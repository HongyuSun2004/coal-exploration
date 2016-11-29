package com.owneriq.wh.spark.processor

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.spark.SparkContextFunctions
import com.owneriq.wh.spark.couchbase.SparkConnector
import com.owneriq.wh.model.event.UserCampaignEventGroup
import com.owneriq.wh.transformer.UserCampaignTransformer
import org.apache.spark.rdd.RDD

object UserCampaignProcessor extends ProcessorTrait{

  //override def loadRDD(sc: SparkContextFunctions, source: String) = {}

  /**
    * Process bid log to generate user campaign event
    * Step 1: build a user campaign event model object for each line of bid log
    * Step 2: aggregate the new user campaign event with same user and ali
    *         and generate a new user campaign event group
    * Step 3: load existing user campaign event group with same user and ali
    *         from couchbase
    * Step 4: merge the new user campaign event group with
    *         existing user campaign event group
    * Step 5: convert the user campaign event group model object to json document
    * @param sc Spark Context Functions
    * @param rdd RDD of flat bid log
    * @return RDD of user campaign event in json document format
    */
  override def processRDD(sc: SparkContextFunctions, rdd : RDD[String]) : RDD[JsonDocument] = {
    /**
      * The function of build user campaign event model object from the flat string
      */
    val buildUserCampaignEventGroup: (String) => UserCampaignEventGroup = (line: String) => {
      val userCampaignEvent = UserCampaignTransformer.transform(line)
      new UserCampaignEventGroup(userCampaignEvent.user, userCampaignEvent.cli.owneriqTargetProfile.advertiserLineItem, List(userCampaignEvent))
    }

    /**
      * The function of loading existing user campaign event group
      * with same user and ali from couchbase and
      * merge the new user campaign event group
      * with existing user campaign event group.
      */
    val mergeWithExistingUserCampaignEventGroup: (SparkContextFunctions, UserCampaignEventGroup) => UserCampaignEventGroup = (sc: SparkContextFunctions, newUserCampaignEventGroup: UserCampaignEventGroup) => {
      //create the user campaign event group key
      val user = newUserCampaignEventGroup.user
      val ali = newUserCampaignEventGroup.ali
      val key = "user_ali_win_event::" + user.id + "::" + ali.id

      //load the existing user campaign event group from couchbase
      val s = SparkConnector.getRDD(sc, key).collect()

      //Build the existing user campaign event group model object from json string
      //Merge new group with existing group
      if(s.length == 1){
        val jsonString = s(0).toString
        val existingUserCampaignEventGroup = UserCampaignTransformer.transformJson(jsonString)
        newUserCampaignEventGroup.merge(existingUserCampaignEventGroup)

      }else{
        newUserCampaignEventGroup
      }

    }

    /**
      * The function of build the couchbase json document with user campaign event group model object
      */
    val buildJsonDocument: (UserCampaignEventGroup) => JsonDocument = (eventGroup: UserCampaignEventGroup) => {
      val user = eventGroup.user
      val ali = eventGroup.ali
      val eventList = eventGroup.eventList

      //convert win event model object to json string
      val eventJsonStringList = eventList.map(UserCampaignTransformer.transformJson)
      //convert json string to couchbase json object
      val eventJsonObjectList = eventJsonStringList.map(JsonObject.fromJson)

      val eventJsonArray = JsonArray.create()
      eventJsonObjectList.foreach(eventJsonArray.add)

      val win_events = JsonObject.create()
        .put("idadvertiserlineitem", ali.id)
        .put("win_list", eventJsonArray)

      val key = "user_ali_win_event::" + user.id + "::" + ali.id

      JsonDocument.create(key,
        JsonObject.create()
          .put("user", user.id)
          .put("win_events", win_events))
    }

    rdd
      .map(buildUserCampaignEventGroup)
      .map(x => ((x.user.id, x.ali.id), x))
      .reduceByKey( (x, y) => x.merge(y) )
      .map(_._2)
      .map(mergeWithExistingUserCampaignEventGroup(sc,_))
      .map(buildJsonDocument)
  }

//  override def saveRDD(sc: SparkContextFunctions, rdd: RDD[JsonDocument]) = {
//    rdd.saveToCouchbase
//  }
}
