package com.owneriq.wh.model.user

class Location (val region: String,
                val country: String,
                val postalCode: String,
                val geoData: String,
                val dmaCode: Long) extends Serializable{

}
