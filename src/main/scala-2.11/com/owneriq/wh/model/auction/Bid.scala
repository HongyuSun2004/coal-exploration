package com.owneriq.wh.model.auction

import com.owneriq.wh.model.campaign.Ad

class Bid (val auction: Auction,
           val scarcityScore: String,
           val requestData: String,
           val date: String,
           val time: Long,
           val isWin: Boolean,
           val bidCPM: Float,
           val pricePaidCPM: Float,
           val ad: Ad,
           val isClick: Boolean) extends Serializable{

}
