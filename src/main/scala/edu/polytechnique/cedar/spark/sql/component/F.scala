package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{
  ReusedSubqueryExec,
  SparkPlan,
  SubqueryBroadcastExec
}
import org.apache.spark.sql.execution.adaptive.{
  BroadcastQueryStageExec,
  ShuffleQueryStageExec
}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

object F {
  def sumLogicalPlanSizeInBytes(plan: LogicalPlan): BigInt = {
    if (plan.children.isEmpty) {
      assert(plan.isInstanceOf[LogicalRelation])
      plan.stats.sizeInBytes
    } else plan.children.map(sumLogicalPlanSizeInBytes).sum
  }

  def sumLogicalPlanRowCount(plan: LogicalPlan): BigInt = {
    if (plan.children.isEmpty) {
      assert(plan.isInstanceOf[LogicalRelation])
      plan.stats.rowCount.getOrElse(0)
    } else plan.children.map(sumLogicalPlanRowCount).sum
  }

  def getUniqueOperatorId(plan: SparkPlan): Int = {
    plan match {
      case p: ShuffleQueryStageExec =>
        p.shuffle.id
      case p: BroadcastQueryStageExec =>
        p.broadcast.id
      case p: SubqueryBroadcastExec =>
        //        p.name.split("#").last.toInt
        p.id
      case p: ReusedSubqueryExec =>
        p.child.id
      //        p.name.split("#").last.toInt
      case p: ReusedExchangeExec =>
        p.child.id
      case p => p.id
    }
  }
}
