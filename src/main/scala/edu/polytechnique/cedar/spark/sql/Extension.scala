package edu.polytechnique.cedar.spark.sql

import edu.polytechnique.cedar.spark.sql
import edu.polytechnique.cedar.spark.sql.DepType.DepType
import edu.polytechnique.cedar.spark.sql.InType.InType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, AdaptiveSparkPlanExec, BroadcastQueryStageExec, ExchangeQueryStageExec, ShuffleQueryStageExec, TableCacheQueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.{FileSourceScanExec, ReusedSubqueryExec, SQLExecution, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryBroadcastExec, SubqueryExec}
import org.json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, pretty, render}
import org.json4s.JValue

import scala.collection.immutable.TreeMap
import scala.collection.{breakOut, mutable}

trait MyUnit {
  def toJson: json4s.JValue
  override def toString: String = pretty(toJson)
  def toCompactString: String = compact(toJson)
}

// node and edges of the operator DAG and queryStage DAG
case class PlanOperator(id: Int, operator: SparkPlan) extends MyUnit {
  private val stats: Statistics = operator.logicalLink match {
    case Some(lgPlan) => lgPlan.stats
    case None         => Statistics(-1)
  }
  private val json = ("id" -> id) ~
    ("name" -> operator.nodeName) ~
    ("className" -> operator.getClass.getName) ~
    ("sizeInBytes" -> stats.sizeInBytes) ~
    ("rowCount" -> stats.rowCount.getOrElse(BigInt(-1))) ~
    ("isRuntime" -> stats.isRuntime) ~
    ("predicate" -> operator.verboseStringWithOperatorId)

  override def toJson: json4s.JValue = {
    render(json)
  }
}

case class OperatorLink(fromId: Int, toId: Int) extends MyUnit {
  private val json = ("fromId" -> fromId) ~ ("toId" -> toId)
  override def toJson: json4s.JValue = {
    render(json)
  }
}

case class InitialPlan(
    operators: TreeMap[Int, PlanOperator],
    links: Seq[OperatorLink],
    inputSizeInBytes: BigInt,
    inputRowCount: BigInt
) extends MyUnit {
  private val operatorMap = operators.map(x => (x._1.toString, x._2.toJson))
  private val linksSeq = links.map(_.toJson)
  private val json = ("operators" -> operatorMap) ~
    ("links" -> linksSeq) ~
    ("inputSizeInBytes" -> inputSizeInBytes) ~
    ("inputRowCount" -> inputRowCount)
  override def toJson: json4s.JValue = {
    render(json)
  }
}

class InitialPlans extends MyUnit {
  private val planMaps =
    mutable.TreeMap[Long, mutable.ArrayBuffer[InitialPlan]]()
  def addInitialPlan(executionId: Long, initialPlan: InitialPlan): Unit = {
    planMaps.get(executionId) match {
      case Some(_) => planMaps(executionId).append(initialPlan)
      case None    => planMaps += (executionId -> mutable.ArrayBuffer(initialPlan))
    }
  }

  def contains(executionId: Long): Boolean = planMaps.contains(executionId)
  override def toJson: JValue = {
    val json = planMaps.map(x => (x._1.toString, x._2.map(_.toJson)))
    render(json)
  }
}

case class PlanQueryStage(
    plan: SparkPlan,
    operators: TreeMap[Int, PlanOperator],
    links: Seq[OperatorLink]
) extends MyUnit {
  private val operatorMap = operators.map(x => (x._1.toString, x._2.toJson))
  private val linksSeq = links.map(_.toJson)
  private val json =
    ("operators" -> operatorMap) ~
      ("links" -> linksSeq) ~
      ("fileSizeInBytes" -> F.sumLeafSizeInBytes(plan, InType.File)) ~
      ("fileRowCount" -> F.sumLeafRowCount(plan, InType.File)) ~
      ("shuffleSizeInBytes" -> F.sumLeafSizeInBytes(plan, InType.Shuffle)) ~
      ("broadcastSizeInBytes" -> F.sumLeafSizeInBytes(
        plan,
        InType.Broadcast
      )) ~
      ("broadcastRowCount" -> F.sumLeafRowCount(plan, InType.Broadcast)) ~
      ("inMemorySizeInBytes" -> F.sumLeafSizeInBytes(plan, InType.InMemory)) ~
      ("inMemoryRowCount" -> F.sumLeafRowCount(plan, InType.InMemory)) ~
      ("numTasks" -> F.getNumTasks(plan))
  override def toJson: JValue = {
    render(json)
  }
}

case class QueryStageLink(fromQSId: Int, toQSId: Int, depType: DepType)
    extends MyUnit {
  private val json =
    ("fromId" -> fromQSId) ~ ("toId" -> toQSId) ~ ("depType" -> depType.toString)
  override def toJson: JValue = {
    render(json)
  }
}
case class CanonQueryStagesLinks(
    fromQSCanon: SparkPlan,
    toQSCanon: SparkPlan,
    depType: DepType
)

case class RuntimePlan(
    canon2QueryStages: mutable.LinkedHashMap[SparkPlan, PlanQueryStage],
    canon2QueryStageIds: mutable.LinkedHashMap[SparkPlan, Int],
    canonQueryStagesLinks: mutable.ArrayBuffer[CanonQueryStagesLinks]
) extends MyUnit {
  var terminated: Boolean = false
  def contains(canon: SparkPlan): Boolean = canon2QueryStages.contains(canon)
  def addQueryStageId(canon: SparkPlan, queryStageId: Int): Unit = {
    if (!canon2QueryStageIds.contains(canon)) {
      canon2QueryStageIds += (canon -> queryStageId)
    }
  }
  def addLink(canon1: SparkPlan, canon2: SparkPlan, depType: DepType): Unit = {
    canonQueryStagesLinks += CanonQueryStagesLinks(canon1, canon2, depType)
  }

  def addQueryStage(canon: SparkPlan, queryStage: PlanQueryStage): Unit = {
    canon2QueryStages += (canon -> queryStage)
    queryStage.operators.values.foreach(planOperator =>
      planOperator.operator match {
        case p: ExchangeQueryStageExec =>
          assert(p.children.isEmpty && p.canonicalized.children.length == 1)
          val childCanon = p.canonicalized.children.head
          val matchedChildCanon = {
            if (canon2QueryStages.contains(childCanon)) {
              childCanon
            } else {
              childCanon.logicalLink match {
                case Some(lgPlan) =>
                  val matchCanons: Seq[SparkPlan] = (for (
                    observedCanon <- canon2QueryStages.keySet
                    if observedCanon.logicalLink.isDefined && observedCanon.logicalLink.get == lgPlan
                  ) yield observedCanon)(breakOut)
                  assert(matchCanons.size == 1)
                  matchCanons.head
                case None =>
                  val matchCanons: Seq[SparkPlan] = (for (
                    canon <- canon2QueryStages.keySet
                    if childCanon == canon.children.head
                  ) yield canon)(breakOut)
                  assert(matchCanons.size == 1)
                  matchCanons.head
              }
            }
          }
          addQueryStageId(
            matchedChildCanon,
            F.getUniqueOperatorId(p)
          ) // found the queryStageId for its precedent queryStage
          addLink(
            matchedChildCanon,
            canon,
            if (p.isInstanceOf[ShuffleQueryStageExec]) DepType.Shuffle
            else DepType.Broadcast
          )
        case p: AdaptiveSparkPlanExec =>
          assert(p.isSubquery)
          val childCanon = p.canonicalized
          addLink(childCanon, canon, DepType.Subquery)
        case p: SubqueryExec =>
        case _: FileSourceScanExec =>
        case _: InMemoryTableScanExec =>
        case p if p.children.isEmpty => throw new Exception(s"${p}")
        case _ =>
      }
    )
  }
  def terminate(): Unit = { terminated = true }

  def unterminate(): Unit = { terminated = false } // for debug

  override def toJson: JValue = {
    if (terminated) {
      var nextNonIdentifiedQueryStageId = canon2QueryStageIds.values.max
      assert(canon2QueryStageIds.keySet.subsetOf(canon2QueryStages.keySet))
      val id2QueryStage = canon2QueryStages.map { case (canon, queryStage) =>
        val queryStageId = canon2QueryStageIds.get(canon) match {
          case Some(id) => id
          case None =>
            nextNonIdentifiedQueryStageId += 1
            canon2QueryStageIds += (canon -> nextNonIdentifiedQueryStageId)
            nextNonIdentifiedQueryStageId
        }
        (queryStageId, queryStage)
      }
      val queryStages = id2QueryStage.map(x => (x._1.toString, x._2.toJson))
      val linksSeq = canonQueryStagesLinks.map(x => {
        val fromQSCanon = x.fromQSCanon match {
          case z if canon2QueryStageIds.contains(z) => z
          case z =>
            // for subquery matching
            val canonCandidates = canon2QueryStageIds.keySet
              .filter(_.verboseStringWithOperatorId() == z.verboseStringWithOperatorId())
            assert(canonCandidates.size == 1)
            canonCandidates.head
        }
        QueryStageLink(
          canon2QueryStageIds(fromQSCanon),
          canon2QueryStageIds(x.toQSCanon),
          x.depType
        ).toJson
      })
      val json =
        ("queryStages" -> queryStages) ~ ("links" -> linksSeq)
      render(json)
    } else {
      render("QS" -> "unfinished")
    }
  }
}

class RuntimePlans extends MyUnit {
  private val planMaps = mutable.TreeMap[Long, RuntimePlan]()

  def terminate(): Unit = {
    planMaps.values.foreach(_.terminate())
  }

  def getOrCreateRuntimePlan(executionId: Long): RuntimePlan = {
    planMaps.get(executionId) match {
      case Some(r) => r
      case None =>
        planMaps += (executionId -> RuntimePlan(
          canon2QueryStages =
            mutable.LinkedHashMap[SparkPlan, PlanQueryStage](),
          canon2QueryStageIds = mutable.LinkedHashMap[SparkPlan, Int](),
          canonQueryStagesLinks = mutable.ArrayBuffer[CanonQueryStagesLinks]()
        ))
        planMaps(executionId)
    }
  }

  override def toJson: JValue = {
    val json = planMaps.map(x => (x._1.toString, x._2.toJson))
    render(json)
  }
}

// time metrics

case class InitialPlanTimeMetric(
    queryStartTimeMap: mutable.TreeMap[Long, Long],
    queryEndTimeMap: mutable.TreeMap[Long, Long]
)

object InType extends Enumeration {
  type InType = Value
  val File, Shuffle, Broadcast, InMemory = Value
}

object DepType extends Enumeration {
  type DepType = Value
  val Shuffle, Broadcast, Subquery = Value

  override def values: sql.DepType.ValueSet = super.values
}

object F {

  // supportive functions

  def getExecutionId(spark: SparkSession): Option[Long] = {
    Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(_.toLong)
  }

  def getUniqueOperatorId(plan: SparkPlan): Int = {
    plan match {
      case p: ShuffleQueryStageExec =>
        p.shuffle.id
      case p: BroadcastQueryStageExec =>
        p.broadcast.id
      case p => p.id
    }
  }

  def showChildren(plan: SparkPlan): Unit = {
    println(plan.nodeName, plan.id)
    plan.children.foreach(showChildren)
  }

  def traversePlan(
      plan: SparkPlan,
      uniqueOperatorIdMap: mutable.TreeMap[Int, Int],
      operators: mutable.TreeMap[Int, PlanOperator],
      links: mutable.ArrayBuffer[OperatorLink],
      rootId: Int,
      mylog: Option[Logger] = None
  ): Unit = {

//    val uniqueOperatorId = F.getUniqueOperatorId(plan)
//    val operatorId = uniqueOperatorIdMap.get(uniqueOperatorId) match {
//      case Some(id) => id
//      case None     => plan.id
//    }
    val uniqueOperatorId = F.getUniqueOperatorId(plan)
    val operatorId = uniqueOperatorId
    if (!operators.contains(operatorId)) {
      operators += (operatorId -> PlanOperator(operatorId, plan))
      plan.children.foreach(
        traversePlan(
          _,
          uniqueOperatorIdMap,
          operators,
          links,
          operatorId,
          mylog
        )
      )
      plan.subqueries.foreach(
        traversePlan(
          _,
          uniqueOperatorIdMap,
          operators,
          links,
          operatorId,
          mylog
        )
      )
      uniqueOperatorIdMap += (uniqueOperatorId -> operatorId)
    }
    if (rootId != -1) {
      links.append(OperatorLink(operatorId, rootId))
      mylog match {
        case Some(l) => l.debug(s"add $operatorId -> $rootId")
        case None    =>
      }
    }
  }

  def getOperatorsAndLinks(
      plan: SparkPlan,
      uniqueOperatorIdMap: mutable.TreeMap[Int, Int],
      mylog: Logger
  ): (mutable.TreeMap[Int, PlanOperator], mutable.ArrayBuffer[OperatorLink]) = {
    mylog.debug("-- traverse plan --")
    val operators = mutable.TreeMap[Int, PlanOperator]()
    val links = mutable.ArrayBuffer[OperatorLink]()
    F.traversePlan(
      plan,
      uniqueOperatorIdMap,
      operators,
      links,
      -1,
      Some(mylog)
    )
    mylog.debug(s"${operators.toString}, ${links.toString}")
    (operators, links)
  }

  def sumLeafSizeInBytes(plan: SparkPlan, inputType: InType): BigInt = {
    if (plan.children.isEmpty) { // if it is a leafNode
      val sizeInBytes = plan.logicalLink.get.stats.sizeInBytes
      plan match {
        case _: BroadcastQueryStageExec =>
          if (inputType == InType.Broadcast) sizeInBytes else 0
        case _: ShuffleQueryStageExec =>
          if (inputType == InType.Shuffle) sizeInBytes else 0
        case _: TableCacheQueryStageExec =>
          if (inputType == InType.InMemory) sizeInBytes else 0
        case _ => if (inputType == InType.File) sizeInBytes else 0
      }
    } else plan.children.map(p => sumLeafSizeInBytes(p, inputType)).sum
  }

  def sumLeafRowCount(plan: SparkPlan, inputType: InType): BigInt = {
    if (plan.children.isEmpty) {
      val rowCount = plan.logicalLink.get.stats.rowCount.getOrElse(BigInt(0))
      plan match {
        case _: BroadcastQueryStageExec =>
          if (inputType == InType.Broadcast) rowCount else 0
        case _: ShuffleQueryStageExec =>
          if (inputType == InType.Shuffle) rowCount else 0
        case _: TableCacheQueryStageExec =>
          if (inputType == InType.InMemory) rowCount else 0
        case _ => if (inputType == InType.File) rowCount else 0
      }

    } else plan.children.map(p => sumLeafRowCount(p, inputType)).sum
  }

  def getFileSourcePartitionNum(f: FileSourceScanExec): Int = {
    // a simplified version (for our TPCH/TPCDS trace collection)
    // f.optionalBucketSet is not defined
    // => val bucketedScan = false
    // => use the logic of `createReadRDD` to simulate the RDD creation and get the latency with light overhead

    val relation = f.relation

    def isDynamicPruningFilter(e: Expression): Boolean =
      e.exists(_.isInstanceOf[PlanExpression[_]])

    // 1. get selectedPartitions
    // 2. assume dynamicallySelectedPartitions = selectedPartitions (verified in most of our trace collections)

    // We can only determine the actual partitions at runtime when a dynamic partition filter is
    // present. This is because such a filter relies on information that is only available at run
    // time (for instance the keys used in the other side of a join).

    val selectedPartitions = relation.location.listFiles(
      f.partitionFilters.filterNot(isDynamicPruningFilter),
      f.dataFilters
    )
    val openCostInBytes =
      f.session.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(f.session, selectedPartitions)

    // derived the functionality from [[org.apache.spark.sql.execution.PartitionedFileUtil.splitFiles]]
    val splitFileSizes = selectedPartitions.flatMap { partition =>
      partition.files.flatMap(file =>
        (0L until file.getLen by maxSplitBytes).map { offset =>
          val remaining = file.getLen - offset
          if (remaining > maxSplitBytes) maxSplitBytes else remaining
        }
      )
    }
    // derived the functionality from [[org.apache.spark.sql.execution.datasources.FilePartition.getFilePartitions]]
    var numPartitions: Int = 0
    var currentSize: Long = 0L

    def closePartition(): Unit = {
      if (currentSize > 0L) {
        numPartitions += 1
      }
      currentSize = 0L
    }

    splitFileSizes.foreach { fileSize =>
      if (currentSize + fileSize > maxSplitBytes) {
        closePartition()
      }
      currentSize += fileSize + openCostInBytes
    }
    closePartition()
    numPartitions
  }

  def getNumTasks(plan: SparkPlan): Int = {
    plan match {
      case p: AQEShuffleReadExec                                => p.partitionSpecs.length
      case _: BroadcastQueryStageExec                           => 1
      case p: ShuffleQueryStageExec                             => p.shuffle.numPartitions
      case f: FileSourceScanExec                                => getFileSourcePartitionNum(f)
      case p if p.getClass.getSimpleName == "HiveTableScanExec" => 1
      case p if p.children.nonEmpty                             => p.children.map(getNumTasks).max
      case p =>
        throw new Exception(
          s"should not reach an unmatched LeafExec ${p.getClass.getName}"
        )
    }
  }

}
// inserted rules for extract traces
case class ExportInitialPlan(
    spark: SparkSession,
    initialPlans: InitialPlans
) extends Rule[SparkPlan] {

  val uniqueOperatorIdMap: mutable.TreeMap[Int, Int] = mutable.TreeMap()
  val mylog: Logger = Logger.getLogger(getClass.getName)
  mylog.setLevel(Level.ERROR)

  def apply(plan: SparkPlan): SparkPlan = {
    val executionId: Long = F.getExecutionId(spark).getOrElse(-1)
    assert(executionId >= 0L)
    if (
      !Status.isCompileTimeMap
        .contains(executionId) || Status.isCompileTimeMap(executionId)
    ) {
      // initialPlan does not contain executionId
      // OR
      // initialPlan contains executionId but Status.isCompileTime => (exists Subqueries)
      val (operators, links) =
        F.getOperatorsAndLinks(
          plan,
          uniqueOperatorIdMap,
          mylog
        )
      val initialPlan = InitialPlan(
        TreeMap(operators.toArray: _*),
        links,
        F.sumLeafSizeInBytes(plan, InType.File),
        F.sumLeafRowCount(plan, InType.File)
      )
      initialPlans.addInitialPlan(executionId, initialPlan)
      Status.isCompileTimeMap.get(executionId) match {
        case Some(_) => Status.isCompileTimeMap.update(executionId, true)
        case None    => Status.isCompileTimeMap += (executionId -> true)
      }
    }
    plan
  }
}

case class ExportRuntimeQueryStage(
    spark: SparkSession,
    runtimePlans: RuntimePlans
) extends Rule[SparkPlan] {

  val uniqueOperatorIdMap: mutable.TreeMap[Int, Int] = mutable.TreeMap()
  val mylog: Logger = Logger.getLogger(getClass.getName)
  mylog.setLevel(Level.ERROR)
  var hitNum = 0

  def apply(plan: SparkPlan): SparkPlan = {

    val executionId: Long = F.getExecutionId(spark).getOrElse(-1)
    assert(Status.isCompileTimeMap.contains(executionId))
    Status.isCompileTimeMap.update(executionId, false)

    val curCanon = plan.canonicalized
    val runtimePlan = runtimePlans.getOrCreateRuntimePlan(executionId)
    hitNum += 1
    println("---", hitNum, "---")

    if (runtimePlan.contains(curCanon)) {
      // found reused plan; do nothing
    } else {
      val (operators, links) =
        F.getOperatorsAndLinks(
          plan,
          uniqueOperatorIdMap,
          mylog
        )
      val queryStage =
        PlanQueryStage(plan, TreeMap(operators.toArray: _*), links)
      println(queryStage.toString)
      runtimePlan.addQueryStage(curCanon, queryStage)
//      runtimePlan.analyzeQueryStage(curCanon, queryStage)
    }
    plan
  }
}

case class AggMetrics() {
  val initialPlans: InitialPlans = new InitialPlans()
  val initialPlanTimeMetric: InitialPlanTimeMetric = InitialPlanTimeMetric(
    queryStartTimeMap =
      mutable.TreeMap[Long, Long](), // executionId to queryStartTime
    queryEndTimeMap =
      mutable.TreeMap[Long, Long]() // executionId to queryEndTime
  )
  val runtimePlans: RuntimePlans = new RuntimePlans()

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

object Status {
  var isCompileTimeMap: mutable.TreeMap[Long, Boolean] =
    mutable.TreeMap[Long, Boolean]()
}
