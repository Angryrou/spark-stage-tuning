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
    // add our TAG!
    plan.setTagValue(F.UDAO_QS_TAG, plan.canonicalized)

    // only expose the query stage with executionId = 1
    if (executionId.get != 1)
      return plan

    // when the plan has been observed, skip it.
    if (rc.observedPhysicalQS.contains(plan.canonicalized)) {
      if (debug) {
        println(
          s"This query stage (${F.getLeafTables(plan)}) has been observed before in plan.canonicalized."
        )
      }
      return plan
    }
    if (rc.observedLogicalQS.contains(plan.logicalLink.get.canonicalized)) {
      if (debug) {
        println(
          "This query stage has been observed before in plan.logicalLink.get.canonicalized."
        )
      }
      return plan
    }

    // when the plan is a reused query stage, skip it.
    plan match {
      case qs: QueryStageExec if qs.plan.isInstanceOf[ReusedExchangeExec] =>
        if (debug) { println("This query stage is a reused query stage.") }
        return plan
      case _ =>
    }

    // expose the query stage
    rc.updateUdaoTag2Metrics(plan, spark)
    rc.observedLogicalQS += plan.logicalLink.get.canonicalized
    rc.observedPhysicalQS += plan.canonicalized

    if (debug) {
      val tables = F.getLeafTables(plan)
      println("----------------------------------------")
      println(
        s"added runtime QS-${rc.getQsId} (tables: ${tables})"
      )
//      println(plan.canonicalized)
    }

    plan
  }
}
