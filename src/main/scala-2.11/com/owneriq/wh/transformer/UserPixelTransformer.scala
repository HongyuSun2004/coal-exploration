package com.owneriq.wh.transformer

import com.owneriq.wh.model.event.UserPixelEvent
//import com.owneriq.wh.model.user.User

object UserPixelTransformer extends TransformerTrait[UserPixelEvent]{
  override def transform(line: String): UserPixelEvent = {
    new UserPixelEvent()
  }

  override def transformJson(jsonString: String): UserPixelEvent = {
    import net.liftweb.json._
    val jValue = parse(jsonString)
    jValue.extract[UserPixelEvent]
  }
}
