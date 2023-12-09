package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class LogicalOperator(plan: LogicalPlan, forQueryStage: Boolean)
    extends MyUnit {

  private val predicate: String =
    plan.verboseStringWithOperatorId().replace("\n", " ")
  val sign: Int = plan.treeString.hashCode()
  val name: String = plan.nodeName
  private val json = if (forQueryStage) {
    val sizeInBytes = plan.stats.sizeInBytes
    val rowCount = plan.stats.rowCount.getOrElse(BigInt(-1))
    val (sizeInBytesEstimated, rowCountEstimated) = if (plan.stats.isRuntime) {
      plan match {
        case lqp: LogicalQueryStage =>
          val statsEstimated = lqp.logicalPlan.stats
          (
            statsEstimated.sizeInBytes,
            statsEstimated.rowCount.getOrElse(BigInt(-1))
          )
        case _ =>
          new Exception("Should be LogicalQueryStage")
      }
    } else {
      (sizeInBytes, rowCount)
    }
    ("sign" -> sign) ~
      ("className" -> plan.getClass.getName) ~
      ("sizeInBytes" -> sizeInBytes) ~
      ("stats" ->
        ("runtime" -> (("sizeInBytes" -> sizeInBytes) ~ ("rowCount" -> rowCount))) ~
        ("compileTime" -> (("sizeInBytes" -> sizeInBytesEstimated) ~ ("rowCount" -> rowCountEstimated)))) ~
      ("isRuntime" -> plan.stats.isRuntime) ~
      ("predicate" -> predicate)
  } else {
    ("sign" -> sign) ~
      ("className" -> plan.getClass.getName) ~
      ("sizeInBytes" -> plan.stats.sizeInBytes) ~
      ("rowCount" -> plan.stats.rowCount.getOrElse(BigInt(-1))) ~
      ("isRuntime" -> plan.stats.isRuntime) ~
      ("predicate" -> predicate)
  }
  override def toJson: JValue = render(json)
}
