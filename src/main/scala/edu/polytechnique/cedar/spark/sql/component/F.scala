package edu.polytechnique.cedar.spark.sql.component

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{
  BinaryNode,
  LeafNode,
  LogicalPlan,
  UnaryNode,
  Union
}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
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

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import org.jgrapht.{Graph, GraphMapping}
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}
import org.jgrapht.alg.isomorphism.{
  IsomorphicGraphMapping,
  VF2SubgraphIsomorphismInspector,
  VF2SubgraphMappingIterator
}

import java.util.Comparator

object F {

  val UDAO_QS_TAG: TreeNodeTag[SparkPlan] =
    TreeNodeTag[SparkPlan]("udao-qs-tag")

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
      mapPartitionDistributionDict: mutable.Map[Int, Array[Long]],
      existedLQPs: Set[LogicalPlan],
      forQueryStage: Boolean
  ): Unit = {
    val logicalOperator = LogicalOperator(plan)
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

  def exposeLQP(
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

  def exposeQSMetrics(
      plan: SparkPlan,
      observedLQPs: Set[LogicalPlan]
  ): QSUnitMetrics = {
    assert(plan.logicalLink.isDefined)
    val lqpQsUnit =
      exposeLQP(plan.logicalLink.get, observedLQPs, forQueryStage = true)
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

    QSUnitMetrics(
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

  def getLeafTables(plan: SparkPlan): String = plan
    .collectLeaves()
    .filter(_.isInstanceOf[FileSourceScanExec])
    .map(_.asInstanceOf[FileSourceScanExec].tableIdentifier.get.table)
    .sorted
    .mkString(",")

  def updateHopMap(
      qsMap: TrieMap[Int, QSUnit],
      id: Int
  ): Seq[(String, Int)] = {
    val qs = qsMap(id)
    if (qs.isFinalHopMap) return qs.hopMap
    val parentHopMap = qs.parentIds
      .flatMap(updateHopMap(qsMap, _))
      .map(x => (x._1, x._2 + 1))
    qs.hopMap = (parentHopMap ++ qs.hopMap).sorted
    qs.isFinalHopMap = true
    qs.hopMap
  }

  def computeHopMap(qsMap: TrieMap[Int, QSUnit]): Unit = {
    val finalQsIdSet = qsMap.keySet -- qsMap.values.flatMap(_.parentIds).toSet
    // iterate over the final Ids of the main query and subqueries (if any)
    finalQsIdSet.foreach(updateHopMap(qsMap, _))
  }

  def serializeHopMap(hopMap: Seq[(String, Int)]): String =
    hopMap.sorted.map(x => x._1 + "x" + x._2.toString).mkString(",")

  case class JGraphNode(id: Int, parentIds: Seq[Int], hopMapSign: String)

  private object NodeOrdering extends Ordering[JGraphNode] {
    override def compare(o1: JGraphNode, o2: JGraphNode): Int =
      o1.hopMapSign.compareTo(o2.hopMapSign)
  }

  private def matchGraphs(
      g1: Graph[JGraphNode, DefaultEdge],
      g2: Graph[JGraphNode, DefaultEdge]
  ): Map[Int, Int] = {

    val inspector =
      new VF2SubgraphIsomorphismInspector(
        g1,
        g2,
        NodeOrdering,
        null
      )

    assert(inspector.isomorphismExists())
    val mappings = inspector.getMappings.asScala
      .asInstanceOf[Iterator[IsomorphicGraphMapping[JGraphNode, DefaultEdge]]]
      .toSeq
    assert(mappings.size == 1)
    val mm =
      mappings.head.getForwardMapping.asScala.map(x => (x._1.id, x._2.id)).toMap
    mm
  }

  def createGraph(
      unitMap: TrieMap[Int, _ <: StageBaseUnit]
  ): Graph[JGraphNode, DefaultEdge] = {
    val jNodeMap = unitMap.map { x =>
      val jNode = JGraphNode(x._1, x._2.parentIds, x._2.getHopMapSign)
      (x._1, jNode)
    }
    val g = new SimpleGraph[JGraphNode, DefaultEdge](classOf[DefaultEdge])
    jNodeMap.values.foreach(g.addVertex)
    jNodeMap.values.foreach { x =>
      x.parentIds.foreach { y => g.addEdge(jNodeMap(y), x) }
    }
    g
  }
  def mappingQS2StageGroup(
      qsMap: TrieMap[Int, _ <: StageBaseUnit],
      sgMap: TrieMap[Int, _ <: StageBaseUnit]
  ): Map[Int, Int] = {
    val g1 = createGraph(qsMap)
    val g2 = createGraph(sgMap)
    matchGraphs(g1, g2)
  }

  def addToQueue(
      mainMetaMap: mutable.TreeMap[(Int, Int), QSIndex],
      subMetaMapGroups: mutable.Map[Int, mutable.TreeMap[(Int, Int), QSIndex]],
      subqueryFinished: mutable.Map[Int, Boolean]
  ): mutable.ArrayBuffer[QSIndex] = {
    val qsExecutionQueue = mutable.ArrayBuffer[QSIndex]()
    mainMetaMap.foreach { case ((waveId, idInWave), qsIndex) =>
      assert(qsIndex.waveId == waveId && qsIndex.idInWave == idInWave)
      if (qsIndex.hasSubqueries) {
        qsIndex.subqueryIds.foreach { subId =>
          if (!subqueryFinished(subId)) {
            addToQueue(
              subMetaMapGroups(subId),
              subMetaMapGroups,
              subqueryFinished
            ).foreach(qsExecutionQueue.append(_))
          }
        }
        qsExecutionQueue.append(qsIndex)
      } else {
        qsExecutionQueue.append(qsIndex)
        if (qsIndex.isLastQueryStage && qsIndex.isSubquery)
          subqueryFinished.update(qsIndex.planId, true)
      }
    }
    qsExecutionQueue
  }

  def mappingQSMeta2StageGroup(
      qsMetaMap: TrieMap[Int, TrieMap[Int, QSIndex]],
      qsMap: TrieMap[Int, QSUnit],
      sgMap: TrieMap[Int, SGUnit]
  ): Map[Int, Int] = {
//    val qsExecutionQueue = mutable.ArrayBuffer[QSIndex]()
    val subqueryFinished = mutable.Map[Int, Boolean]()

    val mainMetaMap = mutable.TreeMap[(Int, Int), QSIndex]()
    val subMetaMapGroups =
      mutable.Map[Int, mutable.TreeMap[(Int, Int), QSIndex]]()
    val waves = qsMetaMap.keySet.toSeq.sorted
    waves.foreach { wave =>
      val idInWaves = qsMetaMap(wave).keySet.toSeq.sorted
      idInWaves.foreach { idInWave =>
        val qsIndex = qsMetaMap(wave)(idInWave)
        assert(qsIndex.idInWave == idInWave && qsIndex.waveId == wave)
        if (qsIndex.isSubquery) {
          if (!subMetaMapGroups.contains(qsIndex.planId))
            subMetaMapGroups += (qsIndex.planId -> mutable.TreeMap())
          subqueryFinished += (qsIndex.planId -> false)
          subMetaMapGroups(qsIndex.planId) += ((wave, idInWave) -> qsIndex)
        } else {
          mainMetaMap += ((wave, idInWave) -> qsIndex)
        }
      }
    }

    val qsExecutionQueue =
      addToQueue(mainMetaMap, subMetaMapGroups, subqueryFinished)

//    val holdingQSIndexBuffer = mutable.ArrayBuffer[QSIndex]()
//    val waves = qsMetaMap.keySet.toSeq.sorted
//    waves.foreach { wave =>
//      val idInWaves = qsMetaMap(wave).keySet.toSeq.sorted
//      idInWaves.foreach { idInWave =>
//        val qsIndex = qsMetaMap(wave)(idInWave)
//        if (qsIndex.subqueryIds.nonEmpty) {
//          qsIndex.subqueryIds.foreach(subqueryFinished.update(_, false))
//          holdingQSIndexBuffer.append(qsIndex)
//        } else {
//          qsExecutionQueue.append(qsIndex)
//          if (qsIndex.isLastQueryStage && qsIndex.isSubquery) {
//            subqueryFinished.update(qsIndex.planId, true)
//            holdingQSIndexBuffer.foreach { qsIndex =>
//              if (qsIndex.subqueryIds.forall(subqueryFinished(_))) {
//                qsExecutionQueue.append(qsIndex)
//              }
//            }
//            holdingQSIndexBuffer --= holdingQSIndexBuffer.filter(qsIndex =>
//              qsIndex.subqueryIds.forall(subqueryFinished(_))
//            )
//          }
//        }
//      }
//    }

    val sgTableList = sgMap.values
      .map(x => (x.sgId, x.hopMap.filter(_._2 == 0).map(_._1).mkString(",")))
      .groupBy(_._2)
      .mapValues(_.map(_._1).toSeq.sorted)

    val mapping = mutable.Map[Int, Int]()

    sgTableList.foreach { case (table, sgIdList) =>
      qsExecutionQueue
        .filter(x =>
          qsMap(x.optimizedStageOrder).hopMap
            .filter(_._2 == 0)
            .map(_._1)
            .mkString(",") == table
        )
        .map(_.optimizedStageOrder)
        .zip(sgIdList)
        .foreach(x => mapping += x)
    }
    assert(mapping.size == qsExecutionQueue.size)

    mapping.toMap
//    val list1 = qsExecutionQueue.map(_.optimizedStageOrder)
//    val list2 = sgMap.keySet.toSeq.sorted
//    assert(list1.size == list2.size)
//    list1.zip(list2).toMap
  }

}
