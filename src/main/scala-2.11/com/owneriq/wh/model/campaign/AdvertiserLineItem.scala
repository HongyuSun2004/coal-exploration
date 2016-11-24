package com.owneriq.wh.model.campaign

class AdvertiserLineItem (val id: Long,
                          val order: Order) extends Serializable{

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case anotherAli: AdvertiserLineItem => this.id == anotherAli.id
      case _ => false
    }
  }
}
