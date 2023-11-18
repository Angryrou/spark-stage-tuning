package edu.polytechnique.cedar.spark.sql.component

import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class QSUnit(
    logicalPlanMetrics: LogicalPlanMetrics,
    physicalPlanMetrics: PhysicalPlanMetrics,
    inputMetaInfo: InputMetaInfo,
    partitionDistribution: Map[Int, Array[Long]]
) extends MyUnit {

  val json: JsonAST.JObject = ("QSLogical" -> logicalPlanMetrics.toJson) ~
    ("QSPhysical" -> physicalPlanMetrics.toJson) ~
    ("IM" -> inputMetaInfo.toJson) ~
    ("PD" -> partitionDistribution.map(x => (x._1.toString, x._2.toList)))

  override def toJson: JValue = render(json)
}
