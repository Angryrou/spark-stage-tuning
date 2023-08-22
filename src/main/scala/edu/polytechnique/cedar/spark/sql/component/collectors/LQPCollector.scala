package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{
  InputMetaInfo,
  LogicalPlanMetrics,
  MyUnit
}
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

import scala.collection.mutable

case class LQPCollector() extends MyUnit {

  val metricsMap: mutable.Map[String, LogicalPlanMetrics] =
    mutable.TreeMap[String, LogicalPlanMetrics]()
  val inputMetaMap: mutable.Map[String, InputMetaInfo] =
    mutable.TreeMap[String, InputMetaInfo]()
  val durationNsMap: mutable.Map[String, Long] = mutable.TreeMap[String, Long]()
  val target = "collect"

  private def summarize =
    ("LQP" -> metricsMap(target).toJson) ~
      ("IM" -> inputMetaMap(target).toJson) ~
      ("LatNs" -> durationNsMap(target))
  override def toJson: JValue = render(summarize)
}
