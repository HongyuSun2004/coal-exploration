package com.owneriq.wh.transformer

import com.owneriq.wh.model.auction.{Auction, Bid, Exchange}
import com.owneriq.wh.model.campaign._
import com.owneriq.wh.model.event.{UserCampaignEvent, UserCampaignEventGroup}
import com.owneriq.wh.model.user.{Channel, Location, User}

object UserCampaignTransformer extends TransformerTrait[UserCampaignEvent] {

  /**
    * Transform one line of HDFS file to a UserCampaignEvent object
    * @param line one line of HDFS file
    * @return UserCampaignEvent object
    */
  override def transform(line: String): UserCampaignEvent = {
    def parseUser(t: Array[String]): User = {
      val id = t(18)

      val channel: Channel = new Channel(1)

      val segment = new Segment(1)
      val segmentList: List[Segment] = List(segment)

      val region = t(5)
      val country = t(4)
      val postalCode = t(17)
      val geoData = t(28)
      val dmaCode = parseLong(t(6))
      val location: Location = new Location(region, country, postalCode, geoData, dmaCode)

      val ipAddress = t(27)
      val cookieTimeStamp = parseLong(t(16))
      val relatedUserList: List[User] = List()

      new User(id, channel, segmentList, location, ipAddress, cookieTimeStamp, relatedUserList)
    }

    def parseBid(t: Array[String], user:User): Bid = {
      val auctionID = t(1)
      val exchange: Exchange = new Exchange(t(0))
      val adSize = t(3)
      val domainName = t(7)
      val auction = new Auction(auctionID, exchange, adSize, domainName, user, user)

      val scarcityScore = t(29)
      val requestData = t(32)
      val date = "date"
      val time = parseLong(t(15))
      val isWin = t(10).toBoolean

      val bidCPM = parseFloat(t(25))
      val pricePaidCPM = parseFloat(t(11))

      val ad: Ad = new Ad(t(31))
      val isClick = t(12).toBoolean

      new Bid(auction, scarcityScore, requestData, date, time, isWin, bidCPM, pricePaidCPM, ad, isClick)
    }

    def parseCampaignLineItem(t: Array[String]): CampaignLineItem = {
      val id = parseLong(t(2))
      val campaignType = parseLong(t(22))
      val targetAudienceType = parseLong(t(23))
      val targetSegment: Segment = new Segment(parseLong(t(9)))

      val advertiser = new Advertiser(parseLong(t(19)))

      val order = new Order(parseLong(t(21)), advertiser)

      val ali = new AdvertiserLineItem(parseLong(t(20)), order)

      val otp = new OwneriqTargetProfile(parseLong(t(24)), ali)

      new CampaignLineItem(id, campaignType, targetAudienceType, targetSegment, otp)

    }

    def parseLong(s: String): Long ={
      if(s.forall(_.isDigit))
        s.toLong
      else
        -1
    }

    def parseFloat(s: String): Float ={
      try{
        s.toFloat
      }catch {
        case _ => -1
      }
    }

    val t = line.split("\t")

    val user = parseUser(t)

    val bid = parseBid(t, user)

    val cli = parseCampaignLineItem(t)

    new UserCampaignEvent(user, cli, bid)

  }

  /**
    * Transform a JSON string to a UserCampaignEventGroup object
    * @param jsonString JSON string
    * @return UserCampaignEventGroup object
    */
  override def transformJson(jsonString: String): UserCampaignEventGroup = {
    import net.liftweb.json._
    val jValue = parse(jsonString)
    jValue.extract[UserCampaignEventGroup]
  }
}


