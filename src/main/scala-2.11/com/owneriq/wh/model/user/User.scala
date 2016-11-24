package com.owneriq.wh.model.user

import com.owneriq.wh.model.campaign.Segment

class User (val id: String,
            val channel: Channel,
            val segmentList: List[Segment],
            val location: Location,
            val ipAddress: String,
            val cookieTimeStamp: Long,
            val relatedUserList: List[User]) extends Serializable{

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case anotherUser: User => this.id == anotherUser.id
      case _ => false
    }
  }
}
