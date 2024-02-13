package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{
  BinaryNode,
  LeafNode,
  LogicalPlan,
  UnaryNode,
  Union
}
import org.apache.spark.sql.execution.adaptive.{
  LogicalQueryStage,
  QueryStageExec,
  ShuffleQueryStageExec
}
import org.apache.spark.sql.execution.{
  BinaryExecNode,
  FileSourceScanExec,
  LeafExecNode,
  SQLExecution,
  SparkPlan,
  UnaryExecNode,
  UnionExec
}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import java.time.{Duration, Instant}

object F {

  type KnobKV = (String, String)

  def getTimeInMs: Long = System.currentTimeMillis()

  def runtime[R](block: => R): (R, Duration) = {
    val startInstant = Instant.now()
    val result = block
    val endInstant = Instant.now()
    val duration = Duration.between(startInstant, endInstant)
    (result, duration)
  }

  def getExecutionId(spark: SparkSession): Option[Long] = {
    Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(_.toLong)
  }

  private def traverseLogical(
      plan: LogicalPlan,
      operators: mutable.TreeMap[Int, LogicalOperator],
      links: mutable.ArrayBuffer[Link],
      signToOpId: mutable.TreeMap[Int, Int],
      linkType: LinkType.LinkType,
      rootId: Int,
      nextOpId: AtomicInteger,
      mapPartitionDistributionDict: mutable.Map[Int, Array[Long]],
      existedLQPs: Set[LogicalPlan],
      forQueryStage: Boolean
  ): Unit = {
    val logicalOperator = LogicalOperator(plan, forQueryStage)
    if (existedLQPs.contains(plan.canonicalized)) { return }

    if (!signToOpId.contains(logicalOperator.sign)) {
      val localOpId = nextOpId.getAndIncrement()
      signToOpId += (logicalOperator.sign -> localOpId)
      operators += (localOpId -> logicalOperator)
      plan match {
        case p: UnaryNode =>
          traverseLogical(
            p.child,
            operators,
            links,
            signToOpId,
            LinkType.Operator,
            localOpId,
            nextOpId,
            mapPartitionDistributionDict,
            existedLQPs,
            forQueryStage
          )
        case p: BinaryNode =>
          p.children.foreach(
            traverseLogical(
              _,
              operators,
              links,
              signToOpId,
              LinkType.Operator,
              localOpId,
              nextOpId,
              mapPartitionDistributionDict,
              existedLQPs,
              forQueryStage
            )
          )
        case lqs: LogicalQueryStage =>
          lqs.physicalPlan match {
            case sqs: ShuffleQueryStageExec =>
              if (sqs.isMaterialized) {
                assert(
                  sqs.mapStats.isDefined,
                  "some failure has already happened, debug needed."
                )
                val shuffleId = sqs.mapStats.get.shuffleId
                val bytesByPartitionId: Array[Long] = sqs.mapStats match {
                  case Some(ms) => ms.bytesByPartitionId
                  case None     => Array[Long]()
                }
                mapPartitionDistributionDict += (shuffleId -> bytesByPartitionId)
              }
            case _ =>
          }
        case _: LeafNode =>
        case u: Union =>
          u.children.foreach(
            traverseLogical(
              _,
              operators,
              links,
              signToOpId,
              LinkType.Operator,
              localOpId,
              nextOpId,
              mapPartitionDistributionDict,
              existedLQPs,
              forQueryStage
            )
          )
        case _ => throw new Exception("sth wrong")
      }
      if (!forQueryStage) {
        plan.subqueries.foreach(
          traverseLogical(
            _,
            operators,
            links,
            signToOpId,
            LinkType.Subquery,
            localOpId,
            nextOpId,
            mapPartitionDistributionDict,
            existedLQPs,
            forQueryStage
          )
        )
      }
    }
    if (rootId != -1) {
      links.append(
        Link(
          signToOpId(logicalOperator.sign),
          logicalOperator.name,
          rootId,
          operators(rootId).name,
          linkType
        )
      )
    }
  }

  private def traversePhysical(
      plan: SparkPlan,
      operators: mutable.TreeMap[Int, PhysicalOperator],
      links: mutable.ArrayBuffer[Link],
      signToOpId: mutable.TreeMap[Int, Int],
      linkType: LinkType.LinkType,
      rootId: Int,
      nextOpId: AtomicInteger,
      mapPartitionDistributionDict: mutable.Map[Int, Array[Long]]
  ): Unit = {
    val physicalOperator = PhysicalOperator(plan)

    if (!signToOpId.contains(physicalOperator.sign)) {
      val localOpId = nextOpId.getAndIncrement()
      signToOpId += (physicalOperator.sign -> localOpId)
      operators += (localOpId -> physicalOperator)
      plan match {
        case p: UnaryExecNode =>
          traversePhysical(
            p.child,
            operators,
            links,
            signToOpId,
            LinkType.Operator,
            localOpId,
            nextOpId,
            mapPartitionDistributionDict
          )
        case p: BinaryExecNode =>
          p.children.foreach(
            traversePhysical(
              _,
              operators,
              links,
              signToOpId,
              LinkType.Operator,
              localOpId,
              nextOpId,
              mapPartitionDistributionDict
            )
          )
        case sqs: ShuffleQueryStageExec =>
          assert(sqs.isMaterialized)
          sqs.mapStats match {
            case Some(ms) =>
              val shuffleId = ms.shuffleId
              val bytesByPartitionId: Array[Long] = ms.bytesByPartitionId
              mapPartitionDistributionDict += (shuffleId -> bytesByPartitionId)
            case None =>
          }
        case _: LeafExecNode =>
        case u: UnionExec =>
          u.children.foreach(
            traversePhysical(
              _,
              operators,
              links,
              signToOpId,
              LinkType.Operator,
              localOpId,
              nextOpId,
              mapPartitionDistributionDict
            )
          )
        case _ => throw new Exception("sth wrong")
      }
//      plan.subqueries.foreach(
//        traversePhysical(
//          _,
//          operators,
//          links,
//          signToOpId,
//          LinkType.Subquery,
//          localOpId,
//          nextOpId,
//          globalSigns
//        )
//      )
    }
    if (rootId != -1) {
      links.append(
        Link(
          signToOpId(physicalOperator.sign),
          physicalOperator.name,
          rootId,
          operators(rootId).name,
          linkType
        )
      )
    }
  }

  def exportLQP(
      plan: LogicalPlan,
      existedLQPs: Set[LogicalPlan] = Set(),
      forQueryStage: Boolean = false
  ): LQPUnit = {
    val operators = mutable.TreeMap[Int, LogicalOperator]()
    val links = mutable.ArrayBuffer[Link]()
    val signToOpId = mutable.TreeMap[Int, Int]()
    val mapPartitionDistributionDict = mutable.Map[Int, Array[Long]]()
    traverseLogical(
      plan,
      operators,
      links,
      signToOpId,
      LinkType.Operator,
      -1,
      new AtomicInteger(0),
      mapPartitionDistributionDict,
      existedLQPs,
      forQueryStage
    )
    val logicalPlanMetrics = LogicalPlanMetrics(
      operators = operators.toMap,
      links = links,
      rawPlan = plan.toString()
    )

    val inputMetaInfo = InputMetaInfo(
      inputSizeInBytes = F.sumLogicalPlanSizeInBytes(plan),
      inputRowCount = F.sumLogicalPlanRowCount(plan)
    )

    LQPUnit(
      logicalPlanMetrics,
      inputMetaInfo,
      mapPartitionDistributionDict.toMap
    )
  }

  def exportQSMetrics(
      plan: SparkPlan,
      observedLQPs: Set[LogicalPlan]
  ): QSMetrics = {
    assert(plan.logicalLink.isDefined)
    val lqpQsUnit =
      exportLQP(plan.logicalLink.get, observedLQPs, forQueryStage = true)
    val operators = mutable.TreeMap[Int, PhysicalOperator]()
    val links = mutable.ArrayBuffer[Link]()
    val signToOpId = mutable.TreeMap[Int, Int]()
    val mapPartitionDistributionDict = mutable.Map[Int, Array[Long]]()
    F.traversePhysical(
      plan,
      operators,
      links,
      signToOpId,
      LinkType.Operator,
      -1,
      new AtomicInteger(0),
      mapPartitionDistributionDict
    )
    val physicalPlanMetrics = PhysicalPlanMetrics(
      operators = operators.toMap,
      links = links,
      rawPlan = plan.toString()
    )

    QSMetrics(
      logicalPlanMetrics = lqpQsUnit.logicalPlanMetrics,
      physicalPlanMetrics = physicalPlanMetrics,
      inputMetaInfo = InputMetaInfo(
        inputSizeInBytes = F.sumPhysicalPlanSizeInBytes(plan),
        inputRowCount = F.sumPhysicalPlanRowCount(plan)
      ),
      initialPartitionNum = plan.outputPartitioning.numPartitions,
      mapPartitionDistributionDict = mapPartitionDistributionDict.toMap
    )
  }

  def sumLogicalPlanSizeInBytes(plan: LogicalPlan): BigInt = {
    plan match {
      case p: LeafNode    => p.stats.sizeInBytes
      case p: LogicalPlan => p.children.map(sumLogicalPlanSizeInBytes).sum
    }
  }

  def sumLogicalPlanRowCount(plan: LogicalPlan): BigInt = {
    plan match {
      case p: LeafNode    => p.stats.rowCount.getOrElse(0)
      case p: LogicalPlan => p.children.map(sumLogicalPlanRowCount).sum
    }
  }

  def sumPhysicalPlanSizeInBytes(plan: SparkPlan): BigInt = {
    plan match {
      case p: QueryStageExec => p.getRuntimeStatistics.sizeInBytes
      case p: LeafExecNode   => p.logicalLink.get.stats.sizeInBytes
      case p: SparkPlan      => p.children.map(sumPhysicalPlanSizeInBytes).sum
    }
  }

  def sumPhysicalPlanRowCount(plan: SparkPlan): BigInt = {
    plan match {
      case p: QueryStageExec => p.getRuntimeStatistics.rowCount.getOrElse(0)
      case p: LeafExecNode   => p.logicalLink.get.stats.rowCount.getOrElse(0)
      case p: SparkPlan      => p.children.map(sumPhysicalPlanRowCount).sum
    }
  }

  private val runtimeKnobsDict = Map(
    "theta_c" -> Seq(
      /* context parameters (theta_c) */
      "spark.executor.cores", // k1
      "spark.executor.memory", // k2
      "spark.executor.instances", // k3
      "spark.default.parallelism", // k4
      "spark.reducer.maxSizeInFlight", // k5
      "spark.shuffle.sort.bypassMergeThreshold", // k6
      "spark.shuffle.compress", // k7
      "spark.memory.fraction" // k8
    ),
    "theta_p" -> Seq(
      /* logical query plan (LQP) parameters (theta_p) */
      "spark.sql.adaptive.advisoryPartitionSizeInBytes", // s1
      "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", // s2
      "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", // s3
      "spark.sql.adaptive.autoBroadcastJoinThreshold", // s4
      "spark.sql.shuffle.partitions", // s5
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", // s6
      "spark.sql.adaptive.skewJoin.skewedPartitionFactor", // s7
      "spark.sql.files.maxPartitionBytes", // s8
      "spark.sql.files.openCostInBytes" // s9
    ),
    "theta_s" -> Seq(
      /* query stage (QS) parameters (theta_s) */
      "spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor", // s10
      "spark.sql.adaptive.coalescePartitions.minPartitionSize" // s11
    )
  )
  private def getConfiguration(
      spark: SparkSession,
      theta_type: String
  ): Array[KnobKV] = {
    assert(runtimeKnobsDict.contains(theta_type))
    val runtimeKnobList: mutable.ArrayBuffer[KnobKV] =
      mutable.ArrayBuffer()
    for (k <- runtimeKnobsDict(theta_type)) {
      runtimeKnobList += ((k, spark.conf.getOption(k).getOrElse("not found")))
    }
    runtimeKnobList.toArray
  }

  def getRuntimeConfiguration(spark: SparkSession): Map[String, Array[KnobKV]] =
    Map(
      "theta_p" -> getConfiguration(spark, "theta_p"),
      "theta_s" -> getConfiguration(spark, "theta_s")
    )

  def getAllConfiguration(spark: SparkSession): Map[String, Array[KnobKV]] =
    Map(
      "theta_c" -> getConfiguration(spark, "theta_c"),
      "theta_p" -> getConfiguration(spark, "theta_p"),
      "theta_s" -> getConfiguration(spark, "theta_s")
    )

  def getLeafTables(plan: SparkPlan): String = plan
    .collectLeaves()
    .filter(_.isInstanceOf[FileSourceScanExec])
    .map(_.asInstanceOf[FileSourceScanExec].tableIdentifier.get.table)
    .sorted
    .mkString(",")

  def aggregateIOBytes(
      stageIds: Seq[Int],
      stageIOBytesDict: TrieMap[Int, IOBytesUnit]
  ): IOBytesUnit = {
    stageIds.map(stageIOBytesDict(_)).reduce { (x, y) =>
      IOBytesUnit(
        inputRead = x.inputRead + y.inputRead,
        inputWritten = x.inputWritten + y.inputWritten,
        shuffleRead = x.shuffleRead + y.shuffleRead,
        shuffleWritten = x.shuffleWritten + y.shuffleWritten
      )
    }
  }

  implicit val formats: DefaultFormats.type = DefaultFormats
  def decodeMessageAndSetconf(message: String, spark: SparkSession): Unit = {
    val json: JValue = parse(message)
    val confKV = json.extract[Map[String, String]]
    println("start setting updated runtime parameters")
    for ((k, v) <- confKV) {
      spark.conf.set(k, v)
    }
  }

}
