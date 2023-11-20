package edu.polytechnique.cedar.spark.sql.extensions

import edu.polytechnique.cedar.spark.sql.component.F
import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

case class ExposeRuntimeQueryStage(
    spark: SparkSession,
    rc: RuntimeCollector,
    debug: Boolean
) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val executionId = F.getExecutionId(spark)
    // just for our experiment -- we do not have duplicated commands.
    assert(
      executionId.isDefined && executionId.get <= 1,
      "Assertion failed: we should not have executionId.isEmpty or executionId > 2"
    )
    // only expose the query stage with executionId = 1
    if (executionId.get != 1)
      return plan

    // when the plan has been observed, skip it.
    if (rc.observedLogicalQS.contains(plan.logicalLink.get.canonicalized)) {
      if (debug) {
        println("This query stage has been observed before.")
      }
      return plan
    }

    // when the plan is a reused query stage, skip it.
    plan match {
      case qs: QueryStageExec if qs.plan.isInstanceOf[ReusedExchangeExec] =>
        println("This query stage is a reused query stage.")
        return plan
      case _ =>
    }
    if (rc.observedPhysicalQS.contains(plan.canonicalized)) {
      println("This query stage can be reused.")
      throw new Exception("should not be reached here")
    }

    // expose the query stage
    val qsId = rc.addQS(
      qsUnit = F.exposeQS(plan, rc.observedLogicalQS.toSet),
      startTimeInMs = F.getTimeInMs,
      snapshot = rc.runtimeStageTaskTracker.snapshot(),
      runtimeKnobsDict = F.getRuntimeConfiguration(spark)
    )
    rc.observedLogicalQS += plan.logicalLink.get.canonicalized
    rc.observedPhysicalQS += plan.canonicalized
    if (debug) {
      println(s"added runtime QS-${qsId} for execId=${executionId.get}")
    }

    plan
  }
}
