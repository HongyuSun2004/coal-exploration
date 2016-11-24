package com.owneriq.wh.model.event

import com.owneriq.wh.model.campaign.{CampaignLineItem}
import com.owneriq.wh.model.auction.Bid
import com.owneriq.wh.model.user.User

class UserCampaignEvent (val user:User, val cli: CampaignLineItem, val win: Bid) extends Serializable{

}
