package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.F.KnobKV
import edu.polytechnique.cedar.spark.sql.component.{F, LQPUnit, MyUnit}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

import scala.collection.concurrent.TrieMap

class CompileTimeCollector extends MyUnit {

  private val lqpDurationInMsMap: TrieMap[String, Long] = new TrieMap()
  private val lqpMap: TrieMap[String, LQPUnit] = new TrieMap()
  private var thetaMap: Option[Map[String, Array[KnobKV]]] = None

  def markConfiguration(spark: SparkSession): Unit = {
    thetaMap = Some(F.getAllConfiguration(spark))
  }

  def setLQP(plan: LogicalPlan, functionName: String = "collect"): Unit = {
    lqpMap += (functionName -> F.exposeLQP(plan))
  }

  def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long
  ): Unit = {
    // add customized traverse function to sort out the plan, linked type and others.
    // test tpch q15-1 for Subquery
    // test tpch q18-1 for Reused Exchange
    // test tpcds q97-1/q98-1 for SubqueryBroadcast
    if (funcName == "command")
      return
    // just for our experiment -- we do not have duplicated commands.
    assert(!lqpDurationInMsMap.contains(funcName))
    // initialCollector.lqpMap += (funcName -> F.exposeLQP(qe.optimizedPlan))
    lqpDurationInMsMap += (funcName -> (durationNs / 1000000))
  }

  override def toJson: JValue = {
    val json = lqpMap("collect").json ~
      ("DurationInMs" -> lqpDurationInMsMap("collect")) ~
      ("Configuration" -> thetaMap.get.map(x => (x._1, x._2.toSeq)))
    render(json)
  }
}
