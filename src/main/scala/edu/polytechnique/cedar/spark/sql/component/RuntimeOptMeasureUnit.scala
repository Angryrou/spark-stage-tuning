package edu.polytechnique.cedar.spark.sql.component

import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class RuntimeOptMeasureUnit(
    endToEndDuration: Long,
    returnMeasure: Map[String, Float]
) extends MyUnit {

  val json: JsonAST.JObject =
    ("EndtoEndInMs" -> endToEndDuration) ~ returnMeasure

  override def toJson: JValue = render(json)
}
