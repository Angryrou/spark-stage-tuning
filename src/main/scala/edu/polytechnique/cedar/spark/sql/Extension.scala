package edu.polytechnique.cedar.spark.sql

import edu.polytechnique.cedar.spark.sql.InputTypes.InputTypes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{
  AQEShuffleReadExec,
  BroadcastQueryStageExec,
  ShuffleQueryStageExec,
  TableCacheQueryStageExec
}
import org.apache.spark.sql.execution.{
  DataSourceScanExec,
  FileSourceScanExec,
  SQLExecution,
  SparkPlan
}

import scala.collection.immutable.TreeMap
import scala.collection.{breakOut, mutable}

// node and edges of the operator DAG and queryStage DAG

case class Operator(
    id: Int,
    name: String,
    className: String,
    sizeInBytes: BigInt,
    rowCount: BigInt,
    isRuntime: Boolean,
    predicate: String
)

case class OperatorLink(fromId: Int, toId: Int)

case class InitialPlan(
    operators: TreeMap[Int, Operator],
    links: Seq[OperatorLink],
    inputSizeInBytes: BigInt,
    inputRowCount: BigInt
)

case class QueryStage(
    queryStageId: Int,
    operators: TreeMap[Int, Operator],
    links: Seq[OperatorLink],
    fileSizeInBytes: BigInt,
    fileRowCount: BigInt,
    shuffleSizeInBytes: BigInt,
    shuffleRowCount: BigInt,
    broadcastSizeInBytes: BigInt,
    broadcastRowCount: BigInt,
    inMemorySizeInBytes: BigInt,
    inMemoryRowCount: BigInt
    // numTasks: Int
)

case class QueryStageLink(fromQSId: Int, toQSId: Int)

case class RuntimePlan(
    queryStages: mutable.TreeMap[Int, QueryStage],
    queryStagesLinks: mutable.ArrayBuffer[QueryStageLink]
)

// time metrics

case class InitialPlanTimeMetric(
    queryStartTimeMap: mutable.TreeMap[Long, Long],
    queryEndTimeMap: mutable.TreeMap[Long, Long]
)

object InputTypes extends Enumeration {
  type InputTypes = Value
  val File, Shuffle, Broadcast, InMemory = Value
}

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
      operators: mutable.TreeMap[Int, Operator],
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
      plan.getClass.getName,
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
        plan.getClass.get,
        newStats.sizeInBytes,
        newStats.rowCount.getOrElse(-1),
        newStats.isRuntime,
        plan.verboseStringWithOperatorId()
      )
    )
  }

  def sumLeafSizeInBytes(plan: SparkPlan, inputType: InputTypes): BigInt = {
    if (plan.children.isEmpty) {
      val sizeInBytes = plan.logicalLink.get.stats.sizeInBytes
      plan match {
        case _: BroadcastQueryStageExec =>
          if (inputType == InputTypes.Broadcast) sizeInBytes else 0
        case _: ShuffleQueryStageExec =>
          if (inputType == InputTypes.Shuffle) sizeInBytes else 0
        case _: TableCacheQueryStageExec =>
          if (inputType == InputTypes.InMemory) sizeInBytes else 0
        case _ => if (inputType == InputTypes.File) sizeInBytes else 0
      }
    } else plan.children.map(p => sumLeafSizeInBytes(p, inputType)).sum
  }

  def sumLeafRowCount(plan: SparkPlan, inputType: InputTypes): BigInt = {
    if (plan.children.isEmpty) {
      val rowCount = plan.logicalLink.get.stats.rowCount.getOrElse(BigInt(0))
      plan match {
        case _: BroadcastQueryStageExec =>
          if (inputType == InputTypes.Broadcast) rowCount else 0
        case _: ShuffleQueryStageExec =>
          if (inputType == InputTypes.Shuffle) rowCount else 0
        case _: TableCacheQueryStageExec =>
          if (inputType == InputTypes.InMemory) rowCount else 0
        case _ => if (inputType == InputTypes.File) rowCount else 0
      }

    } else plan.children.map(p => sumLeafRowCount(p, inputType)).sum
  }

  def getNumTasks(plan: SparkPlan): Int = {
    plan match {
      case p: AQEShuffleReadExec      => p.partitionSpecs.length
      case _: BroadcastQueryStageExec => 0
      case _: ShuffleQueryStageExec =>
        throw new Exception(f"should not reach a ShuffleQueryStageExec;")
      case p: DataSourceScanExec => p.inputRDDs().length
      case p if p.children.isEmpty =>
        throw new Exception("should not reach an unmatched LeafExec")
      case p => p.children.map(getNumTasks).max
    }
  }

}
// inserted rules for extract traces
case class ExportInitialPlan(
    spark: SparkSession,
    initialPlans: mutable.TreeMap[Long, InitialPlan]
) extends Rule[SparkPlan] {

  val mylog: Logger = Logger.getLogger(getClass.getName)
  mylog.setLevel(Level.INFO)

  def apply(plan: SparkPlan): SparkPlan = {
    val executionId: Long = F.getExecutionId(spark).getOrElse(-1)
    assert(executionId >= 0L)
    if (!initialPlans.contains(executionId)) {
      mylog.debug("-- traverse plan --")
      val operators = mutable.TreeMap[Int, Operator]()
      val links = mutable.ArrayBuffer[OperatorLink]()
      F.traversePlan(plan, operators, links, -1, Some(mylog))
      mylog.debug(s"${operators.toString()}, ${links.toString}")

      initialPlans += (executionId -> InitialPlan(
        TreeMap(operators.toArray: _*),
        links,
        F.sumLeafSizeInBytes(plan, InputTypes.File),
        F.sumLeafRowCount(plan, InputTypes.File)
      ))
    }
    plan
  }
}

case class ExportRuntimeQueryStage(
    spark: SparkSession,
    runtimePlans: mutable.TreeMap[Long, RuntimePlan]
) extends Rule[SparkPlan] {

  val mylog: Logger = Logger.getLogger(getClass.getName)
  var queryStageId: Int = 0
  mylog.setLevel(Level.INFO)

  def apply(plan: SparkPlan): SparkPlan = {

    val executionId: Long = F.getExecutionId(spark).getOrElse(-1)
    mylog.debug(s"$executionId, ${queryStageId}, ${plan.id}, ${plan}")

    // get the current queryStage info
    val operators = mutable.TreeMap[Int, Operator]()
    val links = mutable.ArrayBuffer[OperatorLink]()
    F.traversePlan(plan, operators, links, -1, Some(mylog))

    val queryStage: QueryStage = QueryStage(
      queryStageId,
      TreeMap(operators.toArray: _*),
      links,
      fileSizeInBytes = F.sumLeafSizeInBytes(plan, InputTypes.File),
      fileRowCount = F.sumLeafRowCount(plan, InputTypes.File),
      shuffleSizeInBytes = F.sumLeafSizeInBytes(plan, InputTypes.Shuffle),
      shuffleRowCount = F.sumLeafRowCount(plan, InputTypes.Shuffle),
      broadcastSizeInBytes = F.sumLeafSizeInBytes(plan, InputTypes.Broadcast),
      broadcastRowCount = F.sumLeafRowCount(plan, InputTypes.Broadcast),
      inMemorySizeInBytes = F.sumLeafSizeInBytes(plan, InputTypes.InMemory),
      inMemoryRowCount = F.sumLeafRowCount(plan, InputTypes.InMemory)
    )

    val newQueryStageLinks: mutable.ArrayBuffer[QueryStageLink] =
      (for ((operatorId, o) <- operators if o.name contains "QueryStage")
        yield QueryStageLink(operatorId, queryStageId))(breakOut)
    newQueryStageLinks.foreach(qs =>
      if (qs.fromQSId >= qs.toQSId)
        mylog.warn(s"${qs.fromQSId} -> ${qs.toQSId}")
    )

    runtimePlans.get(executionId) match {
      case Some(runtimePlan: RuntimePlan) => {
        assert(!runtimePlan.queryStages.contains(queryStageId))
        runtimePlan.queryStages += (queryStageId -> queryStage)
        runtimePlan.queryStagesLinks ++= newQueryStageLinks
      }
      case None => {
        val queryStages: mutable.TreeMap[Int, QueryStage] =
          mutable.TreeMap[Int, QueryStage](queryStageId -> queryStage)
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
  val initialPlans: mutable.TreeMap[Long, InitialPlan] =
    mutable.TreeMap[Long, InitialPlan]()
  val initialPlanTimeMetric: InitialPlanTimeMetric = InitialPlanTimeMetric(
    queryStartTimeMap =
      mutable.TreeMap[Long, Long](), // executionId to queryStartTime
    queryEndTimeMap = mutable.TreeMap[Long, Long]()
  ) // executionId to queryEndTime

  val runtimePlans: mutable.TreeMap[Long, RuntimePlan] =
    mutable.TreeMap[Long, RuntimePlan]()
  val stageSubmittedTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()
  val stageCompletedTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()
  val stageFirstTaskTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()
  val stageTotalTaskTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()

  var successFlag: Boolean = true
}
