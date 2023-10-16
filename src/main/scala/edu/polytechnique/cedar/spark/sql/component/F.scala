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

}
