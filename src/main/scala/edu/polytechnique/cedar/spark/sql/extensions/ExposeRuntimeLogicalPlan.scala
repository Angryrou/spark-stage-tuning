package edu.polytechnique.cedar.spark.sql.extensions

import edu.polytechnique.cedar.spark.sql.component.F
import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class ExposeRuntimeLogicalPlan(
    spark: SparkSession,
    rc: RuntimeCollector,
    debug: Boolean
) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val executionId = F.getExecutionId(spark)
    // just for our experiment -- we do not have duplicated commands.
    assert(
      executionId.isDefined && executionId.get <= 1,
      "Assertion failed: we should not have executionId.isEmpty or executionId > 2"
    )
    if (executionId.get == 1) {
      val lqpId = rc.addLQP(
        lqpUnit = F.exposeLQP(plan),
        startTimeInMs = F.getTimeInMs,
        snapshot = rc.runtimeStageTaskTracker.snapshot()
      )
      if (debug) {
        println(s"added runtime LQP-${lqpId} for execId=${executionId.get}")
      }
    }
    plan
  }

}
