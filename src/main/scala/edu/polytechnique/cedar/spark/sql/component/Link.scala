package edu.polytechnique.cedar.spark.sql.component

import edu.polytechnique.cedar.spark.sql.component.LinkType.LinkType
import org.json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, pretty, render}
import org.json4s.JValue

case class Link(
    fromId: Int,
    fromName: String,
    toId: Int,
    toName: String,
    linkType: LinkType
) extends MyUnit {
  private val json =
    ("fromId" -> fromId) ~
      ("fromName" -> fromName) ~
      ("toId" -> toId) ~
      ("toName" -> toName) ~
      ("linkType" -> linkType.toString)

  override def toJson: JValue = render(json)
}
