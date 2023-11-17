package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class LogicalOperator(plan: LogicalPlan) extends MyUnit {

  private val predicate: String =
    plan.verboseStringWithOperatorId().replace("\n", " ")
  val sign: Int = plan.treeString.hashCode()
  val name: String = plan.nodeName
  private val json = ("sign" -> sign) ~
    ("className" -> plan.getClass.getName) ~
    ("sizeInBytes" -> plan.stats.sizeInBytes) ~
    ("rowCount" -> plan.stats.rowCount.getOrElse(BigInt(-1))) ~
    ("isRuntime" -> plan.stats.isRuntime) ~
    ("predicate" -> predicate)
  override def toJson: JValue = render(json)
}
