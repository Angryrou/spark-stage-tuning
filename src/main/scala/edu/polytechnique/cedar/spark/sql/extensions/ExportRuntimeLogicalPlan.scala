package edu.polytechnique.cedar.spark.sql.extensions
import edu.polytechnique.cedar.spark.collector.UdaoCollector
import edu.polytechnique.cedar.spark.sql.component.{F, LQPUnit, RunningSnapshot}
import edu.polytechnique.cedar.spark.udao.UdaoClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

case class ExportRuntimeLogicalPlan(
    spark: SparkSession,
    rc: UdaoCollector,
    debug: Boolean,
    udaoClient: Option[UdaoClient] = None
) extends Rule[LogicalPlan] {

  private def encodeMessage(
      lqpUnit: LQPUnit,
      snapshot: RunningSnapshot
  ): String = {
    val jObject =
      ("RequestType" -> "RuntimeLQP") ~
        ("LqpId" -> rc.lqpCollector.getLqpId) ~
        lqpUnit.json ~
        ("RunningQueryStageSnapshot" -> snapshot.toJson) ~
        ("Configuration" -> F
          .getAllConfiguration(spark)
          .map(y => (y._1, y._2.toSeq)))
    compact(render(jObject))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val executionId = F.getExecutionId(spark)
    // just for our experiment -- we do not have duplicated commands.
    assert(
      executionId.isDefined && executionId.get <= 1,
      "Assertion failed: we should not have executionId.isEmpty or executionId > 2"
    )
    if (executionId.get == 1) {
      val lqpUnit = F.exportLQP(plan)
      val snapshot = rc.snapshotCollector.snapshot()
      if (udaoClient.isDefined) {
        val msg = encodeMessage(lqpUnit, snapshot)
        println("!! message prepared and sent for runtime LQP")
        val (response, dt) = udaoClient.get.getUpdateTheta(msg)
        print(
          s" >>> \n, got: $response, took: ${dt.toMillis} ms\n <<<"
        )
      }
      val lqpId = rc.exportRuntimeLogicalPlanBeforeOptimization(
        lqpUnit = lqpUnit,
        startTimeInMs = F.getTimeInMs,
        snapshot = snapshot,
        runtimeKnobsDict = F.getRuntimeConfiguration(spark)
      )
      if (debug) {
        println(s"added runtime LQP-${lqpId} for execId=${executionId.get}")
      }
    }
    plan
  }

}
