package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.component.collectors.InitialCollector
import edu.polytechnique.cedar.spark.sql.component.F
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

case class UDAOQueryExecutionListener(
    initialCollector: InitialCollector,
    debug: Boolean
) extends QueryExecutionListener {
  // capture the features for the initial LQP

  override def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long
  ): Unit = {
    if (debug)
      println("-----", funcName, "-----")
    // add customized traverse function to sort out the plan, linked type and others.
    // test tpch q15-1 for Subquery
    // test tpch q18-1 for Reused Exchange
    // test tpcds q97-1/q98-1 for SubqueryBroadcast
    if (funcName == "command")
      return
    // just for our experiment -- we do not have duplicated commands.
    assert(!initialCollector.lqpMap.contains(funcName))
    initialCollector.lqpMap += (funcName -> F.exposeLQP(qe.optimizedPlan))
    initialCollector.lqpDurationInMsMap += (funcName -> (durationNs / 1000000))
  }

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception
  ): Unit = {}
}
