package com.owneriq.wh.transformer

/**
  * Transformer provide the following methods
  * 1. Transform flat string to model object
  * 2. Transform JSON string to model object
  * 3. Transform model object to JSON string
  * @tparam T Type of the Model object
  */
trait TransformerTrait[T]{

  /**
    * Transform flat string to model object
    * @param line flat string
    * @return model object
    */
  def transform(line: String): T

  /**
    * Transform json string to model object
    * @param jsonString json string
    * @return model object
    */
  def transformJson(jsonString: String): T

  /**
    * Transform model object to json string
    * @param o model object
    * @return json string
    */
  def transformJson(o: T): String = {
    import net.liftweb.json._
    import net.liftweb.json.Serialization.write
    implicit val formats = DefaultFormats

    val ser = write(o)
    ser.toString
  }

}



