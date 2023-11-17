package edu.polytechnique.cedar.spark.sql.component

import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class LogicalPlanMetrics(
    operators: Map[Int, LogicalOperator],
    links: Seq[Link],
    rawPlan: String
) extends MyUnit {
  private val operatorMap =
    operators.map(x => (x._1.toString, x._2.toJson))
  private val linksSeq = links.map(_.toJson)
  private val json = ("operators" -> operatorMap) ~
    ("links" -> linksSeq) ~
    ("rawPlan" -> rawPlan)

  override def toJson: JValue = render(json)
}
