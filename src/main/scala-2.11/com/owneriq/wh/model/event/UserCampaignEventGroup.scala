package com.owneriq.wh.model.event

import com.owneriq.wh.model.campaign.AdvertiserLineItem
import com.owneriq.wh.model.user.User


class UserCampaignEventGroup (val user:User, val ali: AdvertiserLineItem, val eventList: List[UserCampaignEvent]) extends Serializable{

  /**
    * Merge two user campaign event group
    * @param anotherEvent another user campaign event group
    * @return new user campaign event group with merged events
    */
  def merge(anotherEvent: UserCampaignEventGroup): UserCampaignEventGroup = {
    if(this.user == anotherEvent.user && this.ali == anotherEvent.ali){
      new UserCampaignEventGroup(this.user, this.ali, this.eventList ++ anotherEvent.eventList)
    }else{
      this
    }
  }

}
