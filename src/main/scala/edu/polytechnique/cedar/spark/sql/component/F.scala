package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{
  BinaryNode,
  LeafNode,
  LocalRelation,
  LogicalPlan,
  UnaryNode
}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage

import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger

object F {

  def getTimeInMs: Long = System.currentTimeMillis()

  def getExecutionId(spark: SparkSession): Option[Long] = {
    Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(_.toLong)
  }

  def traverseLogical(
      plan: LogicalPlan,
      operators: mutable.TreeMap[Int, LogicalOperator],
      links: mutable.ArrayBuffer[Link],
      signToOpId: mutable.TreeMap[Int, Int],
      linkType: LinkType.LinkType,
      rootId: Int,
      nextOpId: AtomicInteger
  ): Unit = {
    val logicalOperator = LogicalOperator(plan)
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
            nextOpId
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
              nextOpId
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
          nextOpId
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

  def exposeLQP(plan: LogicalPlan): LQPUnit = {
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
      new AtomicInteger(0)
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

  def sumLogicalPlanSizeInBytes(plan: LogicalPlan): BigInt = {
    if (plan.children.isEmpty) {
      assert(
        plan.isInstanceOf[LogicalRelation] || plan
          .isInstanceOf[LogicalQueryStage] || plan.isInstanceOf[LocalRelation],
        plan.getClass
      )
      plan.stats.sizeInBytes
    } else plan.children.map(sumLogicalPlanSizeInBytes).sum
  }

  def sumLogicalPlanRowCount(plan: LogicalPlan): BigInt = {
    if (plan.children.isEmpty) {
      assert(
        plan.isInstanceOf[LogicalRelation] || plan
          .isInstanceOf[LogicalQueryStage] || plan.isInstanceOf[LocalRelation],
        plan.getClass
      )
      plan.stats.rowCount.getOrElse(0)
    } else plan.children.map(sumLogicalPlanRowCount).sum
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
