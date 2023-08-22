package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.component.collectors.InitialCollector
import edu.polytechnique.cedar.spark.sql.component.{
  F,
  InputMetaInfo,
  Link,
  LinkType,
  LogicalOperator,
  LogicalPlanMetrics,
  LQPUnit
}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

case class UDAOQueryPlanListener(initialCollector: InitialCollector)
    extends QueryExecutionListener {
  // capture the features for the initial LQP

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
    F.traverseLogical(
      qe.optimizedPlan,
      operators,
      links,
      signToOpId,
      LinkType.Operator,
      -1,
      new AtomicInteger(0)
    )
    val logicalPlanMetrics = LogicalPlanMetrics(
      operators = operators.toMap,
      links = links,
      rawPlan = qe.optimizedPlan.toString()
    )

    val inputMetaInfo = InputMetaInfo(
      inputSizeInBytes = F.sumLogicalPlanSizeInBytes(qe.optimizedPlan),
      inputRowCount = F.sumLogicalPlanRowCount(qe.optimizedPlan)
    )

    // just for our experiment -- we do not have duplicated commands.
    assert(!initialCollector.lqpMap.contains(funcName))
    initialCollector.lqpMap += (funcName -> LQPUnit(
      logicalPlanMetrics = logicalPlanMetrics,
      inputMetaInfo = inputMetaInfo
    ))
    initialCollector.lqpLatsMap += (funcName -> durationNs)
  }

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception
  ): Unit = {}
}
