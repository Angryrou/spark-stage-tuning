package edu.polytechnique.cedar.spark.sql.extensions
import edu.polytechnique.cedar.spark.collector.UdaoCollector
import edu.polytechnique.cedar.spark.sql.component.F
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec.TEMP_OPTIMIZED_STAGE_ORDER_TAG
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

case class ExportRuntimeQueryStage(
    spark: SparkSession,
    rc: UdaoCollector,
    debug: Boolean
) extends Rule[SparkPlan] {

  private val observedLogicalQS = mutable.Set[LogicalPlan]()
  private val observedPhysicalQS = mutable.Set[SparkPlan]()
  val canon2IdMap: TrieMap[SparkPlan, Int] = new TrieMap()

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
    if (observedPhysicalQS.contains(plan.canonicalized)) {
      if (debug) {
        println(
          s"This query stage (${F.getLeafTables(plan)}) has been observed before in plan.canonicalized."
        )
      }
      assert(canon2IdMap.contains(plan.canonicalized))
      plan.setTagValue(
        TEMP_OPTIMIZED_STAGE_ORDER_TAG,
        canon2IdMap(plan.canonicalized)
      )
      return plan
    }
    if (observedLogicalQS.contains(plan.logicalLink.get.canonicalized)) {
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

    // export the query stage
    val optId =
      rc.exportRuntimeQueryStageBeforeOptimization(plan, spark, observedLogicalQS.toSet)

    plan.setTagValue(TEMP_OPTIMIZED_STAGE_ORDER_TAG, optId)
    canon2IdMap += (plan.canonicalized -> optId)
    observedLogicalQS += plan.logicalLink.get.canonicalized
    observedPhysicalQS += plan.canonicalized

    if (debug) {
      val tables = F.getLeafTables(plan)
      println("----------------------------------------")
      println(
        s"added runtime QS-${optId} (tables: ${tables})"
      )
    }

    plan
  }
}
