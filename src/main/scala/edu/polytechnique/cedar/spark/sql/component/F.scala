package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{
  BinaryNode,
  LeafNode,
  LocalRelation,
  LogicalPlan,
  UnaryNode
}
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{
  BinaryExecNode,
  LeafExecNode,
  SQLExecution,
  SparkPlan,
  UnaryExecNode
}
import org.apache.spark.sql.execution.adaptive.{
  LogicalQueryStage,
  QueryStageExec
}

import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.Map

object F {

  def getTimeInMs: Long = System.currentTimeMillis()

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
      existedLQPs: Set[LogicalPlan]
  ): Unit = {
    val logicalOperator = LogicalOperator(plan)
    if (existedLQPs.contains(plan)) { return }
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
            existedLQPs
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
              existedLQPs
            )
          )
        case _: LeafNode =>
        case _           => throw new Exception("sth wrong")
      }
      plan.subqueries.foreach(
        traverseLogical(
          _,
          operators,
          links,
          signToOpId,
          LinkType.Subquery,
          localOpId,
          nextOpId,
          existedLQPs
        )
      )
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
      nextOpId: AtomicInteger
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
            nextOpId
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
              nextOpId
            )
          )
        case _: LeafExecNode =>
        case _               => throw new Exception("sth wrong")
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

  def exposeLQP(
      plan: LogicalPlan,
      existedLQPs: Set[LogicalPlan] = Set()
  ): LQPUnit = {
    val operators = mutable.TreeMap[Int, LogicalOperator]()
    val links = mutable.ArrayBuffer[Link]()
    val signToOpId = mutable.TreeMap[Int, Int]()
    F.traverseLogical(
      plan,
      operators,
      links,
      signToOpId,
      LinkType.Operator,
      -1,
      new AtomicInteger(0),
      existedLQPs
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

    LQPUnit(logicalPlanMetrics, inputMetaInfo)
  }

  def exposeQS(
      plan: SparkPlan,
      observedLQPs: Set[LogicalPlan]
  ): QSUnit = {
    assert(plan.logicalLink.isDefined)
    val lqpQsUnit = exposeLQP(plan.logicalLink.get, observedLQPs)
    val operators = mutable.TreeMap[Int, PhysicalOperator]()
    val links = mutable.ArrayBuffer[Link]()
    val signToOpId = mutable.TreeMap[Int, Int]()
    F.traversePhysical(
      plan,
      operators,
      links,
      signToOpId,
      LinkType.Operator,
      -1,
      new AtomicInteger(0)
    )
    val physicalPlanMetrics = PhysicalPlanMetrics(
      operators = operators.toMap,
      links = links,
      rawPlan = plan.toString()
    )

    QSUnit(
      logicalPlanMetrics = lqpQsUnit.logicalPlanMetrics,
      physicalPlanMetrics = physicalPlanMetrics,
      inputMetaInfo = InputMetaInfo(
        inputSizeInBytes = F.sumPhysicalPlanSizeInBytes(plan),
        inputRowCount = F.sumPhysicalPlanRowCount(plan)
      )
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
      case p: LeafExecNode => p.logicalLink.get.stats.sizeInBytes
      case p: SparkPlan    => p.children.map(sumPhysicalPlanSizeInBytes).sum
    }
  }

  def sumPhysicalPlanRowCount(plan: SparkPlan): BigInt = {
    plan match {
      case p: LeafExecNode => p.logicalLink.get.stats.rowCount.getOrElse(0)
      case p: SparkPlan    => p.children.map(sumPhysicalPlanRowCount).sum
    }
  }

  private val runtimeKnobsDict = Map(
    "theta_c" -> Seq(
      /* context parameters (theta_c) */
      "spark.executor.memory", // k1
      "spark.executor.cores", // k2
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
  ): Array[(String, String)] = {
    assert(runtimeKnobsDict.contains(theta_type))
    val runtimeKnobList: mutable.ArrayBuffer[(String, String)] =
      mutable.ArrayBuffer()
    for (k <- runtimeKnobsDict(theta_type)) {
      runtimeKnobList += ((k, spark.conf.getOption(k).getOrElse("not found")))
    }
    runtimeKnobList.toArray
  }

  def getRuntimeConfiguration(
      spark: SparkSession
  ): Map[String, Array[(String, String)]] =
    Map(
      "theta_p" -> getConfiguration(spark, "theta_p"),
      "theta_s" -> getConfiguration(spark, "theta_s")
    )

  def getAllConfiguration(
      spark: SparkSession
  ): Map[String, Array[(String, String)]] =
    Map(
      "theta_c" -> getConfiguration(spark, "theta_c"),
      "theta_p" -> getConfiguration(spark, "theta_p"),
      "theta_s" -> getConfiguration(spark, "theta_s")
    )
}
