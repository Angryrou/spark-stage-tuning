package edu.polytechnique.cedar.spark.collector
import org.apache.spark.scheduler.{
  SparkListenerJobStart,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd,
  SparkListenerTaskStart
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ui.{
  SparkListenerOnQueryStageSubmitted,
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart
}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}

class UdaoCollector(verbose: Boolean = true) {

  val compileTimeCollector = new CompileTimeCollector()
  val lqpCollector = new RuntimeLogicalQueryPlanCollector()
  val sgCollector = new RuntimeSparkStageGroupCollector(verbose)
  val qsCollector = new RuntimeQueryStageCollector(verbose)
  val snapshotCollector = new RuntimeSnapshotCollector()
  private var sqlStartTimeInMs: Long = -1
  private var sqlEndTimeInMs: Long = -1

  def onCompile(spark: SparkSession, queryContent: String): Unit =
    compileTimeCollector.onCompile(spark, queryContent)

  def onJobStart(jobStart: SparkListenerJobStart): Unit =
    sgCollector.onJobStart(jobStart)

  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    snapshotCollector.onStageSubmitted(stageSubmitted)
    sgCollector.onStageSubmitted(stageSubmitted)
  }

  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    sgCollector.onStageCompleted(stageCompleted)

  def onTaskStart(taskStart: SparkListenerTaskStart): Unit =
    snapshotCollector.onTaskStart(taskStart)

  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    snapshotCollector.onTaskEnd(taskEnd)
    sgCollector.onTaskEnd(taskEnd)
  }

  def onSQLExecutionStart(e: SparkListenerSQLExecutionStart): Unit = {
    e.executionId match {
      case 1 => sqlStartTimeInMs = e.time
      case 0 =>
      case _ => new Exception("Should have executionId <= 1")
    }
  }

  def onSQLExecutionEnd(e: SparkListenerSQLExecutionEnd): Unit = {
    assert(
      e.errorMessage.isEmpty || e.errorMessage.get.isEmpty,
      e.errorMessage
    )
    e.executionId match {
      case 1 => sqlEndTimeInMs = e.time
      case 0 =>
      case _ => new Exception("Should have executionId <= 1")
    }
  }

  def exposeQueryStageForOptimization(
      plan: SparkPlan,
      spark: SparkSession,
      observedLogicalQS: Set[LogicalPlan]
  ): Int = {
    qsCollector.exposeQueryStageForOptimization(
      plan,
      spark,
      observedLogicalQS,
      snapshotCollector.snapshot()
    )
  }

  def onQueryStageSubmitted(e: SparkListenerOnQueryStageSubmitted): Unit =
    qsCollector.onQueryStageSubmitted(e)

  def dump2String: String = {
    assert(
      sqlStartTimeInMs > 0 && sqlEndTimeInMs > 0,
      "Assertion failed: cannot be exposed before sqlStartTimeInMs & sqlEndTimeInMs are defined."
    )

    val lqpMap = lqpCollector.exposeMap(sqlEndTimeInMs)
    val sgMap = sgCollector.getStageGroupMap
    val sgResultsMap = sgCollector.aggregateResults
    val qsMap = qsCollector.getQueryStageMap(sgMap, sgResultsMap)

    qsMap
      .map(x => (x._1, x._2.qsOptId, x._2.relevantStages, x._2.table))
      .toSeq
      .sortBy(_._1)
      .foreach(x =>
        println(
          s"QueryStageId: ${x._1} \t OptimizationOrder: ${x._2} \t RelevantStages: ${x._3} \t table: ${x._4}"
        )
      )

    val json: JsonAST.JObject = {
      ("CompileTimeLQP" -> compileTimeCollector.exposeJson) ~
        ("RuntimeLQPs" -> lqpMap) ~
        ("RuntimeQSs" -> qsMap.map(x => (x._1.toString, x._2.json))) ~
        ("SQLStartTimeInMs" -> sqlStartTimeInMs) ~
        ("SQLEndTimeInMs" -> sqlEndTimeInMs) ~
        ("Objectives" -> (
          ("DurationInMs" -> (sqlEndTimeInMs - sqlStartTimeInMs)) ~
            ("IOBytes" -> sgCollector.aggregateAll().json)
        ))

    }
    pretty(render(json))
  }

}
