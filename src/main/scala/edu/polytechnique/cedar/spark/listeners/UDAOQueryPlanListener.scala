package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.component.{
  AggMetrics,
  F,
  InputMetaInfo,
  Link,
  LinkType,
  LogicalOperator,
  LogicalPlanMetrics
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  BinaryNode,
  LeafNode,
  LogicalPlan,
  UnaryNode
}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable

case class UDAOQueryPlanListener(aggMetrics: AggMetrics)
    extends QueryExecutionListener {

  var curOpId: Int = 0

  def traverseLogical(
      plan: LogicalPlan,
      operators: mutable.TreeMap[Int, LogicalOperator],
      links: mutable.ArrayBuffer[Link],
      signToOpId: mutable.TreeMap[Int, Int],
      linkType: LinkType.LinkType,
      rootId: Int
  ): Unit = {
    val logicalOperator = LogicalOperator(plan)
    if (!signToOpId.contains(logicalOperator.sign)) {
      curOpId += 1
      val localOpId = curOpId
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
            localOpId
          )
        case p: BinaryNode =>
          p.children.foreach(
            traverseLogical(
              _,
              operators,
              links,
              signToOpId,
              LinkType.Operator,
              localOpId
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
          localOpId
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

  override def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long
  ): Unit = {
    println("-----", funcName, "-----")
    // add customized traverse function to sort out the plan, linked type and others.
    // test tpch q15-1 for Subquery
    // test tpch q18-1 for Reused Exchange
    // test tpcds q97-1/q98-1 for SubqueryBroadcast
    if (funcName == "command")
      return

    val operators = mutable.TreeMap[Int, LogicalOperator]()
    val links = mutable.ArrayBuffer[Link]()
    val signToOpId = mutable.TreeMap[Int, Int]()
    curOpId = -1
    traverseLogical(
      qe.optimizedPlan,
      operators,
      links,
      signToOpId,
      LinkType.Operator,
      -1
    )
    val logicalPlanMetrics = LogicalPlanMetrics(
      operators = operators,
      links = links,
      rawPlan = qe.optimizedPlan.toString()
    )

    val inputMetaInfo = InputMetaInfo(
      inputSizeInBytes = F.sumLogicalPlanSizeInBytes(qe.optimizedPlan),
      inputRowCount = F.sumLogicalPlanRowCount(qe.optimizedPlan)
    )

    // just for our experiment -- we do not have duplicated commands.
    assert(!aggMetrics.logicalPlanMetricsMap.contains(funcName))
    aggMetrics.logicalPlanMetricsMap += (funcName -> logicalPlanMetrics)
    aggMetrics.planInputMetaMap += (funcName -> inputMetaInfo)
  }

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception
  ): Unit = {}
}
