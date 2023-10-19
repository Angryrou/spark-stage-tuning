package edu.polytechnique.cedar.spark.sql.extensions

import edu.polytechnique.cedar.spark.sql.component.F
import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable

case class ExposeRuntimeLogicalPlan(
    spark: SparkSession,
    rc: RuntimeCollector,
    debug: Boolean
) extends Rule[LogicalPlan] {

  private val runtimeKnobNames = Seq(
    /* logical query plan (LQP) parameters (theta_p) */
    "spark.sql.adaptive.advisoryPartitionSizeInBytes", // s1
    "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", // s2
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", // s3
    "spark.sql.adaptive.autoBroadcastJoinThreshold", // s4
    "spark.sql.shuffle.partitions", // s5
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", // s6
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor", // s7

    /* query stage (QS) parameters (theta_s) */
    "spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor", // s8
    "spark.sql.adaptive.coalescePartitions.minPartitionSize" // s9
  )

  private def getRuntimeConfiguration = {
    val runtimeKnobList: mutable.ArrayBuffer[(String, String)] =
      mutable.ArrayBuffer()
    for (k <- runtimeKnobNames) {
      runtimeKnobList += ((k, spark.conf.getOption(k).getOrElse("not found")))
    }
    runtimeKnobList.toArray
  }

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
        snapshot = rc.runtimeStageTaskTracker.snapshot(),
        runtimeKnobList = getRuntimeConfiguration
      )
      if (debug) {
        println(s"added runtime LQP-${lqpId} for execId=${executionId.get}")
      }
    }
    plan
  }

}
