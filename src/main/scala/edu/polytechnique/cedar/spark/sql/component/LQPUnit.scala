package edu.polytechnique.cedar.spark.sql.component

import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class LQPUnit(
    logicalPlanMetrics: LogicalPlanMetrics,
    inputMetaInfo: InputMetaInfo,
    mapPartitionDistributionDict: Map[Int, Array[Long]]
) extends MyUnit {

  val json: JsonAST.JObject = ("LQP" -> logicalPlanMetrics.toJson) ~
    ("IM" -> inputMetaInfo.toJson) ~
    ("PD" -> mapPartitionDistributionDict.map(x =>
      (x._1.toString, x._2.toList)
    ))

  override def toJson: JValue = render(json)
}
