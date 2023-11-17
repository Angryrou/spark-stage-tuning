package edu.polytechnique.cedar.spark.sql.extensions

import edu.polytechnique.cedar.spark.sql.component.F
import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

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
    if (executionId.get == 1) {
      val qsId = rc.addQS(
        qsUnit = F.exposeQS(plan, rc.globalLogicalSigns),
        startTimeInMs = F.getTimeInMs,
        snapshot = rc.runtimeStageTaskTracker.snapshot(),
        runtimeKnobsDict = F.getRuntimeConfiguration(spark)
      )
      if (debug) {
        println(s"added runtime QS-${qsId} for execId=${executionId.get}")
      }
    }
    plan
  }
}
