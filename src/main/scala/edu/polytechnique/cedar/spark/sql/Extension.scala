package edu.polytechnique.cedar.spark.sql

import edu.polytechnique.cedar.spark.sql
import edu.polytechnique.cedar.spark.sql.DepType.DepType
import edu.polytechnique.cedar.spark.sql.InType.InType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{
  AQEShuffleReadExec,
  AdaptiveSparkPlanExec,
  BroadcastQueryStageExec,
  ExchangeQueryStageExec,
  ShuffleQueryStageExec,
  TableCacheQueryStageExec
}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.{
  FileSourceScanExec,
  SQLExecution,
  SparkPlan,
  SubqueryExec
}
import org.json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, pretty, render}
import org.json4s.JValue

import scala.collection.immutable.TreeMap
import scala.collection.mutable

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

case class QueryStageSign(plan: SparkPlan) {
  private val p = plan match {
    case a: AQEShuffleReadExec => a.child
    case p                     => p
  }

  // for QueryStages in the main query
  private val verboseUp = {
    var s = ""
    p.foreachUp {
      case _: AQEShuffleReadExec     =>
      case _: ExchangeQueryStageExec =>
      case pp                        => s += pp.verboseStringWithOperatorId()
    }
    s
  }

  // for QueryStages with subqueries
  private val verboseString = p.verboseStringWithOperatorId()

  def id: Int = p.id
  def global: String = verboseUp
  def subquery: String = verboseString

  override def toString: String = (global + "||" + subquery)
}

case class QueryStageLink(fromQSId: Int, toQSId: Int, depType: DepType)
    extends MyUnit {
  private val json =
    ("fromId" -> fromQSId) ~ ("toId" -> toQSId) ~ ("depType" -> depType.toString)
  override def toJson: JValue = {
    render(json)
  }
}
case class SignQueryStageLink(
    fromQSSign: String,
    toQSSign: String,
    depType: DepType
)
case class SubquerySignQueryStageLink(
    fromSubQSSign: String,
    toSubQSSign: String,
    depType: DepType
)

case class RuntimePlan(
    sign2QueryStages: mutable.LinkedHashMap[String, PlanQueryStage],
    subquerySignList: mutable.ArrayBuffer[String],
    signLinks: mutable.ArrayBuffer[SignQueryStageLink],
    subquerySignLinks: mutable.ArrayBuffer[SubquerySignQueryStageLink]
) extends MyUnit {
  var terminated: Boolean = false
  def contains(signStr: String): Boolean = sign2QueryStages.contains(signStr)
  def addLink(
      signStr1: String,
      signStr2: String,
      depType: DepType,
      isSubquery: Boolean
  ): Unit = {
    if (!isSubquery) {
      signLinks += SignQueryStageLink(
        signStr1,
        signStr2,
        depType
      )
    } else {
      subquerySignLinks += SubquerySignQueryStageLink(
        signStr1,
        signStr2,
        depType
      )
    }

  }

  def addQueryStage(sign: QueryStageSign, queryStage: PlanQueryStage): Unit = {
    assert(!sign2QueryStages.contains(sign.global))
    sign2QueryStages += (sign.global -> queryStage)
    subquerySignList.append(sign.subquery)

    queryStage.operators.values.foreach(planOperator =>
      planOperator.operator match {
        case p: ExchangeQueryStageExec =>
          assert(p.children.isEmpty && p.canonicalized.children.length == 1)
          val childSign = QueryStageSign(p.canonicalized.children.head)
          if (!sign2QueryStages.contains(childSign.global))
            println("debug")
          addLink(
            childSign.global,
            sign.global,
            if (p.isInstanceOf[ShuffleQueryStageExec]) DepType.Shuffle
            else DepType.Broadcast,
            isSubquery = false
          )
        case p: AdaptiveSparkPlanExec =>
          // match shadow verboseString for subqueries
          assert(p.isSubquery)
          val childSign = QueryStageSign(p.canonicalized)
          addLink(
            childSign.subquery,
            sign.subquery,
            DepType.Subquery,
            isSubquery = true
          )
        case _: SubqueryExec          =>
        case _: FileSourceScanExec    =>
        case _: InMemoryTableScanExec =>
        case p if p.children.isEmpty  => throw new Exception(s"${p}")
        case _                        =>
      }
    )
  }
  def terminate(): Unit = { terminated = true }

  override def toJson: JValue = {
    if (terminated) {
      val queryStages = sign2QueryStages.values.zipWithIndex.map {
        case (queryStage, i) => (i.toString, queryStage.toJson)
      }
      val signStr2QueryStageIds = sign2QueryStages.keySet.zipWithIndex.toMap

      val linksSeq = signLinks.map(x =>
        QueryStageLink(
          signStr2QueryStageIds(x.fromQSSign),
          signStr2QueryStageIds(x.toQSSign),
          x.depType
        ).toJson
      ) ++ subquerySignLinks.map(x =>
        QueryStageLink(
          subquerySignList.indexOf(x.fromSubQSSign),
          subquerySignList.indexOf(x.toSubQSSign),
          x.depType
        ).toJson
      )
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
          sign2QueryStages = mutable.LinkedHashMap[String, PlanQueryStage](),
          subquerySignList = mutable.ArrayBuffer[String](),
          signLinks = mutable.ArrayBuffer[SignQueryStageLink](),
          subquerySignLinks = mutable.ArrayBuffer[SubquerySignQueryStageLink]()
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
      uniqueOperatorIdSet: mutable.Set[Int],
      operators: mutable.TreeMap[Int, PlanOperator],
      links: mutable.ArrayBuffer[OperatorLink],
      rootId: Int,
      mylog: Option[Logger] = None
  ): Unit = {

    // use the unique operator Id to
    // (1) identify resued operators and
    // (2) avoid id conflicts for QueryStage with subqueries
    val operatorId = F.getUniqueOperatorId(plan)
    if (!operators.contains(operatorId)) {
      operators += (operatorId -> PlanOperator(operatorId, plan))
      plan.children.foreach(
        traversePlan(
          _,
          uniqueOperatorIdSet,
          operators,
          links,
          operatorId,
          mylog
        )
      )
      plan.subqueries.foreach(
        traversePlan(
          _,
          uniqueOperatorIdSet,
          operators,
          links,
          operatorId,
          mylog
        )
      )
      uniqueOperatorIdSet.add(operatorId)
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
      uniqueOperatorIdSet: mutable.Set[Int],
      mylog: Logger
  ): (mutable.TreeMap[Int, PlanOperator], mutable.ArrayBuffer[OperatorLink]) = {
    mylog.debug("-- traverse plan --")
    val operators = mutable.TreeMap[Int, PlanOperator]()
    val links = mutable.ArrayBuffer[OperatorLink]()
    F.traversePlan(
      plan,
      uniqueOperatorIdSet,
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

  val uniqueOperatorIdSet: mutable.Set[Int] = mutable.Set()
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
          uniqueOperatorIdSet,
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

  val uniqueOperatorIdSet: mutable.Set[Int] = mutable.Set()
  val mylog: Logger = Logger.getLogger(getClass.getName)
  mylog.setLevel(Level.ERROR)
  var hitNum = 0

  def apply(plan: SparkPlan): SparkPlan = {

    val executionId: Long = F.getExecutionId(spark).getOrElse(-1)
    assert(Status.isCompileTimeMap.contains(executionId))
    Status.isCompileTimeMap.update(executionId, false)
    val sign = QueryStageSign(plan.canonicalized)
    val runtimePlan = runtimePlans.getOrCreateRuntimePlan(executionId)
    hitNum += 1
    println("---", hitNum, "---")

    if (runtimePlan.contains(sign.global)) {
      // found reused plan; do nothing
    } else {
      val (operators, links) =
        F.getOperatorsAndLinks(
          plan,
          uniqueOperatorIdSet,
          mylog
        )
      val queryStage =
        PlanQueryStage(plan, TreeMap(operators.toArray: _*), links)
//      println(queryStage.toString)
      println(sign.id, sign.global.hashCode())
      runtimePlan.addQueryStage(sign, queryStage)
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
