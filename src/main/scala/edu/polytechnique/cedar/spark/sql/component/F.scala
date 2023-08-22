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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  BinaryNode,
  LeafNode,
  LogicalPlan,
  UnaryNode
}

import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger

object F {

  def traverseLogical(
      plan: LogicalPlan,
      operators: mutable.TreeMap[Int, LogicalOperator],
      links: mutable.ArrayBuffer[Link],
      signToOpId: mutable.TreeMap[Int, Int],
      linkType: LinkType.LinkType,
      rootId: Int,
      nextOpId: AtomicInteger
  ): Unit = {
    val logicalOperator = LogicalOperator(plan)
    if (!signToOpId.contains(logicalOperator.sign)) {
      val localOpId = nextOpId.getAndIncrement()
      signToOpId += (logicalOperator.sign -> localOpId)
      operators += (localOpId -> logicalOperator)
      plan match {
        case p: UnaryNode =>
          traverseLogical(
            p.child,
            operators,
            links,
            signToOpId,
            LinkType.Operator,
            localOpId,
            nextOpId
          )
        case p: BinaryNode =>
          var offset = 1
          p.children.foreach(
            traverseLogical(
              _,
              operators,
              links,
              signToOpId,
              LinkType.Operator,
              localOpId,
              nextOpId
            )
          )
        case _: LeafNode =>
        case _           => throw new Exception("sth wrong")
      }
      plan.subqueries.foreach(
        traverseLogical(
          _,
          operators,
          links,
          signToOpId,
          LinkType.Subquery,
          localOpId,
          nextOpId
        )
      )
    }
    if (rootId != -1) {
      links.append(
        Link(
          signToOpId(logicalOperator.sign),
          logicalOperator.name,
          rootId,
          operators(rootId).name,
          linkType
        )
      )
    }
  }

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

}
