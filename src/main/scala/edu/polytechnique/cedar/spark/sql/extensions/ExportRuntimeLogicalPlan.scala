package edu.polytechnique.cedar.spark.sql.extensions
import edu.polytechnique.cedar.spark.collector.UdaoCollector
import edu.polytechnique.cedar.spark.sql.component.{
  F,
  LQPUnit,
  RunningSnapshot,
  RuntimeOptMeasureUnit
}
import edu.polytechnique.cedar.spark.udao.UdaoClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

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
        ("TemplateId" -> rc.getTemplateId) ~
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
      var runtimeOptMeasureUnit: RuntimeOptMeasureUnit =
        RuntimeOptMeasureUnit(
          endToEndDuration = 0,
          returnMeasure = Map[String, Float]()
        )
      if (
        udaoClient.isDefined &&
        lqpUnit.logicalPlanMetrics.operators
          .map(x => x._2.name)
          .toSeq
          .contains("Join") &&
        !lqpUnit.logicalPlanMetrics.operators
          .map(x =>
            x._2.name
              .equals("LogicalQueryStage") && !x._2.plan.stats.isRuntime
          )
          .exists(b => b) // skip if LQP is not ready
      ) {
        val msg = encodeMessage(lqpUnit, snapshot)
        println("!! message prepared and sent for runtime LQP")
        val (response, dt) = udaoClient.get.getUpdateTheta(msg)
        print(
          s" >>> \n, got: $response, took: ${dt.toMillis} ms\n <<<"
        )
        val measure = F.decodeMessageAndSetconf(response, spark)
        runtimeOptMeasureUnit = RuntimeOptMeasureUnit(
          endToEndDuration = dt.toMillis,
          returnMeasure = measure
        )
      }

      val lqpId = rc.exportRuntimeLogicalPlanBeforeOptimization(
        lqpUnit = lqpUnit,
        startTimeInMs = F.getTimeInMs,
        snapshot = snapshot,
        runtimeOptMeasureUnit = runtimeOptMeasureUnit,
        runtimeKnobsDict = F.getRuntimeConfiguration(spark)
      )
      if (debug) {
        println(s"added runtime LQP-${lqpId} for execId=${executionId.get}")
      }
    }
    plan
  }

}
