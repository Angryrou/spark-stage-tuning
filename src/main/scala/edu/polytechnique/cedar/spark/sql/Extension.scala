package edu.polytechnique.cedar.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}

import scala.collection.{breakOut, mutable}

// node and edges of the operator DAG and queryStage DAG

case class Operator(
    id: Int,
    name: String,
    sizeInBytes: BigInt,
    rowCount: BigInt,
    isRuntime: Boolean,
    predicate: String
)

case class OperatorLink(fromId: Int, toId: Int)

case class InitialPlan(
    operators: Map[Int, Operator],
    links: Seq[OperatorLink],
    inputSizeInBytes: BigInt,
    inputRowCount: BigInt
)

case class QueryStage(
    queryStageId: Int,
    operators: Map[Int, Operator],
    links: Seq[OperatorLink],
    inputSizeInBytes: BigInt,
    inputRowCount: BigInt
)

case class QueryStageLink(fromQSId: Int, toQSId: Int)

case class RuntimePlan(
    queryStages: mutable.Map[Int, QueryStage],
    queryStagesLinks: mutable.ArrayBuffer[QueryStageLink]
)

// time metrics

case class InitialPlanTimeMetric(
    queryStartTimeMap: mutable.Map[Long, Long],
    queryEndTimeMap: mutable.Map[Long, Long]
)

case class RuntimePlanTimeMetric(
    queryStageStartTimeMap: mutable.Map[Long, mutable.Map[Int, Long]],
    queryStageEndTimeMap: mutable.Map[Long, mutable.Map[Int, Long]]
)

object F {

  // supportive functions

  def getExecutionId(spark: SparkSession): Option[Long] = {
    Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(_.toLong)
  }

  def showChildren(plan: SparkPlan): Unit = {
    println(plan.nodeName, plan.id)
    plan.children.foreach(showChildren)
  }

  def traversePlan(
      plan: SparkPlan,
      operators: mutable.Map[Int, Operator],
      links: mutable.ArrayBuffer[OperatorLink],
      rootId: Int,
      mylog: Option[Logger] = None
  ): Unit = {
    val operatorId = plan.id
    val stats: Statistics = plan.logicalLink match {
      case Some(lgPlan) => lgPlan.stats
      case None         => Statistics(-1)
    }

    val operator = Operator(
      operatorId,
      plan.nodeName,
      stats.sizeInBytes,
      stats.rowCount.getOrElse(-1),
      stats.isRuntime,
      plan.verboseStringWithOperatorId()
    )
    operators += (operatorId -> operator)

    if (rootId != -1) {
      links.append(OperatorLink(operatorId, rootId))
      mylog match {
        case Some(l) => l.debug(s"add $operatorId -> $rootId")
        case None    =>
      }
    }

    plan.children.foreach(traversePlan(_, operators, links, operatorId))
    val newStats: Statistics = plan.logicalLink match {
      case Some(lgPlan) => lgPlan.stats
      case None         => Statistics(-1)
    }

    operators.update(
      operatorId,
      Operator(
        operatorId,
        plan.nodeName,
        newStats.sizeInBytes,
        newStats.rowCount.getOrElse(-1),
        newStats.isRuntime,
        plan.verboseStringWithOperatorId()
      )
    )
  }

  def sumLeafInputSizeInBytes(plan: SparkPlan): BigInt = if (
    plan.children.isEmpty
  ) plan.logicalLink.get.stats.sizeInBytes
  else plan.children.map(p => sumLeafInputSizeInBytes(p)).sum

  def sumLeafInputRowCount(plan: SparkPlan): BigInt = if (plan.children.isEmpty)
    plan.logicalLink.get.stats.rowCount.getOrElse(0)
  else plan.children.map(p => sumLeafInputRowCount(p)).sum
}

// inserted rules for extract traces
case class ExportInitialPlan(
    spark: SparkSession,
    initialPlans: mutable.Map[Long, InitialPlan]
) extends Rule[SparkPlan] {

  val mylog: Logger = Logger.getLogger(getClass.getName)
  mylog.setLevel(Level.INFO)

  def apply(plan: SparkPlan): SparkPlan = {
    val executionId: Long = F.getExecutionId(spark).getOrElse(-1)
    assert(executionId >= 0L)
    if (!initialPlans.contains(executionId)) {
      mylog.debug("-- traverse plan --")
      val operators = mutable.Map[Int, Operator]()
      val links = mutable.ArrayBuffer[OperatorLink]()
      F.traversePlan(plan, operators, links, -1, Some(mylog))
      mylog.debug(s"${operators.toString()}, ${links.toString}")

      initialPlans += (executionId -> InitialPlan(
        operators.toMap,
        links,
        F.sumLeafInputSizeInBytes(plan),
        F.sumLeafInputRowCount(plan)
      ))
    }
    plan
  }
}

case class ExportRuntimeQueryStage(
    spark: SparkSession,
    runtimePlans: mutable.Map[Long, RuntimePlan]
) extends Rule[SparkPlan] {

  val mylog: Logger = Logger.getLogger(getClass.getName)
  var queryStageId: Int = 0
  mylog.setLevel(Level.INFO)

  def apply(plan: SparkPlan): SparkPlan = {
    val executionId: Long = F.getExecutionId(spark).getOrElse(-1)
    mylog.debug(s"$executionId, ${queryStageId}, ${plan.id}, ${plan}")

    // get the current queryStage info
    val operators = mutable.Map[Int, Operator]()
    val links = mutable.ArrayBuffer[OperatorLink]()
    F.traversePlan(plan, operators, links, -1, Some(mylog))

    val queryStage: QueryStage = QueryStage(
      queryStageId,
      operators.toMap,
      links,
      F.sumLeafInputSizeInBytes(plan),
      F.sumLeafInputRowCount(plan)
    )
    val newQueryStageLinks: mutable.ArrayBuffer[QueryStageLink] =
      (for ((operatorId, o) <- operators if o.name contains "QueryStage")
        yield QueryStageLink(operatorId, queryStageId))(breakOut)
    newQueryStageLinks.foreach(qs => assert(qs.fromQSId < qs.toQSId))

    runtimePlans.get(executionId) match {
      case Some(runtimePlan: RuntimePlan) => {
        assert(!runtimePlan.queryStages.contains(queryStageId))
        runtimePlan.queryStages += (queryStageId -> queryStage)
        runtimePlan.queryStagesLinks ++= newQueryStageLinks
      }
      case None => {
        val queryStages: mutable.Map[Int, QueryStage] =
          mutable.Map[Int, QueryStage](queryStageId -> queryStage)
        val queryStageLinks: mutable.ArrayBuffer[QueryStageLink] =
          newQueryStageLinks
        val runtimePlan: RuntimePlan = RuntimePlan(queryStages, queryStageLinks)
        runtimePlans += (executionId -> runtimePlan)
      }
    }
    queryStageId += 1
    plan
  }
}

case class AggMetrics() {
  val initialPlans: mutable.Map[Long, InitialPlan] =
    mutable.Map[Long, InitialPlan]()
  val initialPlanTimeMetric: InitialPlanTimeMetric = InitialPlanTimeMetric(
    queryStartTimeMap =
      mutable.Map[Long, Long](), // executionId to queryStartTime
    queryEndTimeMap = mutable.Map[Long, Long]()
  ) // executionId to queryEndTime

  val runtimePlans: mutable.Map[Long, RuntimePlan] =
    mutable.Map[Long, RuntimePlan]()
  val stageSubmittedTime: mutable.Map[(Int, Int), Long] =
    mutable.Map[(Int, Int), Long]() // (stageId, stageAttemptId) -> timeStamp
  val stageCompletedTime: mutable.Map[(Int, Int), Long] =
    mutable.Map[(Int, Int), Long]() // (stageId, stageAttemptId) -> timeStamp
  val stageFirstTaskTime: mutable.Map[(Int, Int), Long] =
    mutable.Map[(Int, Int), Long]() // (stageId, stageAttemptId) -> timeStamp
  val stageTotalTaskTime: mutable.Map[(Int, Int), Long] =
    mutable.Map[(Int, Int), Long]() // (stageId, stageAttemptId) -> duration

  var successFlag: Boolean = true
}
