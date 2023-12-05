package edu.polytechnique.cedar.spark.sql.component

import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class IOBytesUnit(
    inputRead: Long,
    inputWritten: Long,
    shuffleRead: Long,
    shuffleWritten: Long
) extends MyUnit {

  val json: JsonAST.JObject =
    ("Total" -> (inputRead + inputWritten + shuffleRead + shuffleWritten)) ~
      ("Detail" -> (
        ("IR" -> inputRead) ~
          ("IW" -> inputWritten) ~
          ("SR" -> shuffleRead) ~
          ("SW" -> shuffleWritten)
      ))
  override def toJson: JValue = render(json)
}
