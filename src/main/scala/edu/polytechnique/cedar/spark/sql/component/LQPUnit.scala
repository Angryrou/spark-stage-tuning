package edu.polytechnique.cedar.spark.sql.component
import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class LQPUnit(
    logicalPlanMetrics: LogicalPlanMetrics,
    inputMetaInfo: InputMetaInfo
) extends MyUnit {

  val json: JsonAST.JObject = ("LQP" -> logicalPlanMetrics.toJson) ~
    ("IM" -> inputMetaInfo.toJson)
  override def toJson: JValue = render(json)
}
