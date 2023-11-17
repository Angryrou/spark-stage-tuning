package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class PhysicalOperator(plan: SparkPlan) extends MyUnit {

  private val predicate: String =
    plan.verboseStringWithOperatorId().replace("\n", " ")
  val sign: Int = plan.treeString.hashCode()
  val name: String = plan.nodeName
  val stats: Statistics = plan.logicalLink match {
    case Some(lgPlan) => lgPlan.stats
    case None         => Statistics(-1)
  }
  private val json = ("sign" -> sign) ~
    ("className" -> plan.getClass.getName) ~
    ("sizeInBytes" -> stats.sizeInBytes) ~
    ("rowCount" -> stats.rowCount.getOrElse(BigInt(-1))) ~
    ("isRuntime" -> stats.isRuntime) ~
    ("predicate" -> predicate)
  override def toJson: JValue = render(json)
}
