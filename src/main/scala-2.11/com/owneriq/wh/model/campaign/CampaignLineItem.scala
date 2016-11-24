package com.owneriq.wh.model.campaign

class CampaignLineItem (val id: Long,
                        val campaignType: Long,
                        val targetAudienceType: Long,
                        val targetSegment: Segment,
                        val owneriqTargetProfile: OwneriqTargetProfile) extends Serializable{

}
