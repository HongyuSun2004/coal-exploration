package com.owneriq.wh.model.auction

import com.owneriq.wh.model.user.User

class Auction(val id: String,
              val exchange: Exchange,
              val adSize: String,
              val domainName: String,
              val auctionUser: User,
              val owneriqUser: User) extends Serializable{

}
