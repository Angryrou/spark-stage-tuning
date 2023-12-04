package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{
  F,
  LQPUnit,
  QSIndex,
  QSUnit,
  RunningQueryStageSnapshot
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{
  FileSourceScanExec,
  SparkPlan,
  SubqueryExec
}
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  BroadcastQueryStageExec,
  QueryStageExec,
  ShuffleQueryStageExec
}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.ui.SparkListenerOnQueryStageSubmitted
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class RuntimeCollector() {

  // LQP related
  private val lqpId: AtomicInteger = new AtomicInteger(1)
  private val lqpMap: TrieMap[Int, LQPUnit] = new TrieMap[Int, LQPUnit]()
  private val lqpStartTimeInMsMap: TrieMap[Int, Long] = new TrieMap[Int, Long]()
  private val lqpSnapshot: TrieMap[Int, RunningQueryStageSnapshot] =
    new TrieMap[Int, RunningQueryStageSnapshot]()
  private val lqpThetaR: TrieMap[Int, Map[String, Array[(String, String)]]] =
    new TrieMap[Int, Map[String, Array[(String, String)]]]()

  private var sqlStartTimeInMs: Long = -1
  private var sqlEndTimeInMs: Long = -1

  // QS related
  private val qsId: AtomicInteger = new AtomicInteger(0)
  private val qsId2Tag: TrieMap[Int, SparkPlan] = new TrieMap()
  private val qsTag2Id: TrieMap[SparkPlan, Int] = new TrieMap()
  private val qsExchangeId2Tag: TrieMap[Int, SparkPlan] = new TrieMap()
  private val qsMap: TrieMap[Int, QSUnit] = new TrieMap()
  private val unresolvedSubqueries: TrieMap[Int, Seq[AdaptiveSparkPlanExec]] =
    new TrieMap()

  val observedLogicalQS: mutable.Set[LogicalPlan] = mutable.Set[LogicalPlan]()
  val observedPhysicalQS: mutable.Set[SparkPlan] = mutable.Set[SparkPlan]()

  // runtime related
  val runtimeSnapshotTracker = new RuntimeSnapshotTracker()

  // Spark stage related
  val runtimeDependencyTracker = new RuntimeDependencyTracker()

  def getLqpId: Int = lqpId.get()
  def getQsId: Int = qsId.get()

  def addLQP(
      lqpUnit: LQPUnit,
      startTimeInMs: Long,
      snapshot: RunningQueryStageSnapshot,
      runtimeKnobsDict: Map[String, Array[(String, String)]]
  ): Int = {
    val curId = lqpId.getAndIncrement()
    lqpMap += (curId -> lqpUnit)
    lqpStartTimeInMsMap += (curId -> startTimeInMs)
    lqpSnapshot += (curId -> snapshot)
    lqpThetaR += (curId -> runtimeKnobsDict)
    curId
  }

  private val tableCounter = new TrieMap[String, Int]()

  def updateUdaoTag2Metrics(plan: SparkPlan, spark: SparkSession): Int = {
    val curId = qsId.getAndIncrement()
    val qsTag = plan.canonicalized
    assert(!qsTag2Id.contains(qsTag))
    qsId2Tag += (curId -> qsTag)
    qsTag2Id += (qsTag -> curId)

    val parentTags = plan
      .collectLeaves()
      .filter(_.isInstanceOf[QueryStageExec])
      .map(x => getUdaoTag(x.asInstanceOf[QueryStageExec]))
    parentTags.foreach(x => assert(qsTag2Id.contains(x)))
    val parentIds = parentTags.map(qsTag2Id(_))

    // shuffle
    val shuffleParentTags = plan
      .collectLeaves()
      .filter(_.isInstanceOf[ShuffleQueryStageExec])
      .map(x => getUdaoTag(x.asInstanceOf[ShuffleQueryStageExec]))
    shuffleParentTags.foreach(x => assert(qsTag2Id.contains(x)))
    val shuffleParentIds = shuffleParentTags.map(qsTag2Id(_))

    // broadcast
    val broadcastParentTags = plan
      .collectLeaves()
      .filter(_.isInstanceOf[BroadcastQueryStageExec])
      .map(x => getUdaoTag(x.asInstanceOf[BroadcastQueryStageExec]))
    broadcastParentTags.foreach(x => assert(qsTag2Id.contains(x)))
    val broadcastParentIds = broadcastParentTags.map(qsTag2Id(_))

    assert(parentIds.toSet == (shuffleParentIds ++ broadcastParentIds).toSet)

    if (plan.subqueriesAll.nonEmpty) {
      unresolvedSubqueries += (curId -> plan.subqueriesAll.map { sub =>
        assert(sub.isInstanceOf[SubqueryExec])
        val sub1 = sub.asInstanceOf[SubqueryExec]
        assert(sub1.child.isInstanceOf[AdaptiveSparkPlanExec])
        sub1.child.asInstanceOf[AdaptiveSparkPlanExec]
      })
    }

    val initialHopMap = plan
      .collectLeaves()
      .filter(_.isInstanceOf[FileSourceScanExec])
      .map(_.asInstanceOf[FileSourceScanExec].tableIdentifier.get.table)
      .map { tableName =>
        tableCounter.get(tableName) match {
          case Some(count) =>
            tableCounter.update(tableName, count + 1)
            (tableName + (count + 1).toString, 0)
          case None =>
            tableCounter.update(tableName, 1)
            (tableName + "1", 0)
        }
      }
      .sorted

    qsMap += (curId -> QSUnit(
      qsId = curId,
      tag = qsTag,
      shuffleParentIds = shuffleParentIds,
      shuffleParentTags = shuffleParentTags,
      broadcastParentIds = broadcastParentIds,
      broadcastParentTags = broadcastParentTags,
      qsMetrics = F.exposeQSMetrics(plan, observedLogicalQS.toSet),
      hopMap = initialHopMap,
      isFinalHopMap = false,
      optimizationTimeInMs = F.getTimeInMs,
      stageSnapshot = runtimeSnapshotTracker.snapshot(),
      thetaR = F.getRuntimeConfiguration(spark)
    ))

    curId
  }

  private def getUdaoTag(queryStage: QueryStageExec): SparkPlan = {
    queryStage.plan match {
      case reused: ReusedExchangeExec =>
        assert(qsExchangeId2Tag.contains(reused.child.id))
        qsExchangeId2Tag(reused.child.id)
      case qs =>
        val planTag = qs.collectFirst {
          case p if p.getTagValue(F.UDAO_QS_TAG).isDefined =>
            p.getTagValue(F.UDAO_QS_TAG).get
        }
        assert(planTag.isDefined)
        qsExchangeId2Tag += (qs.id -> planTag.get)
        planTag.get
    }
  }

  val qsMetaMap: TrieMap[Int, TrieMap[Int, QSIndex]] = new TrieMap()

  def addQSMeta(e: SparkListenerOnQueryStageSubmitted): Unit = {
    e.optimizedStageOrder match {
      case Some(optimizedStageOrder) =>
        val qsIndex = QSIndex(
          waveId = e.waveId,
          idInWave = e.idInWave,
          planId = e.planId,
          qsId = e.qsId,
          optimizedStageOrder = optimizedStageOrder,
          subqueryIds = e.subqueryIds,
          isSubquery = e.isSubquery,
          isLastQueryStage = e.isLastQueryStage
        )
        qsMetaMap.get(e.waveId) match {
          case Some(waveMetaMap) =>
            assert(!waveMetaMap.contains(e.idInWave))
            waveMetaMap += (e.idInWave -> qsIndex)
          case None =>
            val waveMetaMap = new TrieMap[Int, QSIndex]()
            waveMetaMap += (e.idInWave -> qsIndex)
            qsMetaMap += (e.waveId -> waveMetaMap)
        }
        println(s"addQSMeta: $qsIndex")
      case None =>
        println(
          s"skip: wave:${e.waveId}-${e.idInWave} in plan.${e.planId}->QS.${e.qsId}"
        )
    }
  }

  def setSQLStartTimeInMs(timeInMs: Long): Unit = {
    sqlStartTimeInMs = timeInMs
  }
  def setSQLEndTimeInMs(timeInMs: Long): Unit = {
    sqlEndTimeInMs = timeInMs
  }

  def lqpMapJsonStr: String = {
    assert(
      sqlStartTimeInMs > 0 && sqlEndTimeInMs > 0,
      "Assertion failed: cannot be exposed before sqlStartTimeInMs & sqlEndTimeInMs are defined."
    )
    val lqpMap2 = lqpMap.map(x =>
      (
        x._1.toString,
        x._2.json ~
          ("RunningQueryStageSnapshot" -> lqpSnapshot(x._1).toJson) ~
          ("StartTimeInMs" -> lqpStartTimeInMsMap(x._1)) ~
          ("DurationInMs" -> (sqlEndTimeInMs - lqpStartTimeInMsMap(x._1))) ~
          ("RuntimeConfiguration" -> lqpThetaR(x._1).map(y =>
            (y._1, y._2.toSeq)
          ))
      )
    )

    val sgMap = runtimeDependencyTracker.stageGroupMap
    assert(
      qsMap.size == sgMap.size,
      s"Assertion failed: ${qsMap.size} != ${sgMap.size}"
    )

    val mapping = F.mappingQSMeta2StageGroup(qsMetaMap, qsMap, sgMap)
    qsMap
      .map(x => (x._1, sgMap(mapping(x._1)).stageIds.toList))
      .toSeq
      .sortBy(_._1)
      .foreach(println)

    /*

    N.B. (1) do not include subqueries in the dependency because Spark stages does not have the information.
         (2) actually we do not need hopMap to match isomorphism graphs, but it is very helpful for debugging.

    // translate subqueries into parentIds
    unresolvedSubqueries.toSeq
      .sortBy(_._1)
      .foreach { case (curId, subqueries) =>
        val subqueryTags = subqueries.map { aqe: AdaptiveSparkPlanExec =>
          assert(aqe.isFinalPlan)
          val planTag = aqe.finalPhysicalPlan.collectFirst {
            case p if p.getTagValue(F.UDAO_QS_TAG).isDefined =>
              p.getTagValue(F.UDAO_QS_TAG).get
          }
          assert(planTag.isDefined)
          assert(qsTag2Id.contains(planTag.get))
          planTag.get
        }
        val subqueryIds = subqueryTags.map(qsTag2Id(_))
        assert(qsMap.contains(curId))
        qsMap(curId).updateWithSubqueries(subqueryIds, subqueryTags)
      }
     */

    // construct hopMap for each query stage by (1) find the root node, (2) post-order traverse the tree
    // compute the hopMap to form the label of each node => speedup matching.
    F.computeHopMap(qsMap)
    val qsHopMaps = qsMap.toSeq
      .groupBy(x => F.serializeHopMap(x._2.hopMap))
      .mapValues(_.map(_._1).sorted)
    val sgHopMaps = runtimeDependencyTracker.stageGroupMap.toSeq
      .groupBy(x => F.serializeHopMap(x._2.hopMap))
      .mapValues(_.map(_._1).sorted)
    println("qsHopMaps", qsHopMaps.toSeq.sortBy(_._1))
    println("sgHopMaps", sgHopMaps.toSeq.sortBy(_._1))
    assert(
      qsHopMaps.map(x => (x._1, x._2.size)).toSeq.sorted ==
        sgHopMaps.map(x => (x._1, x._2.size)).toSeq.sorted
    )

    val qsId2sgIdMapping = F.mappingQS2StageGroup(qsMap, sgMap)
    val qsId2QSResultTimes =
      runtimeDependencyTracker.getQsId2QSResultTimes(qsId2sgIdMapping)

    assert(
      qsMap.keySet == qsMetaMap.values
        .flatMap(_.values.map(_.optimizedStageOrder))
        .toSet
    )
    val mapping2 = F.mappingQSMeta2StageGroup(qsMetaMap, qsMap, sgMap)

    val l1 = qsId2sgIdMapping.toSeq.sortBy(_._2).map(_._1)
    val l2 = mapping2.toSeq.sortBy(_._2).map(_._1)

    if (l1 != l2) {
      println("--- qsId2sgIdMapping ---")
      qsMap
        .map(x => (x._1, sgMap(qsId2sgIdMapping(x._1)).stageIds.toList))
        .toSeq
        .sortBy(_._1)
        .foreach(println)

      println("--- qsId2sgIdMapping2 ---")
      qsMap
        .map(x => (x._1, sgMap(mapping2(x._1)).stageIds.toList))
        .toSeq
        .sortBy(_._1)
        .foreach(println)
    }
    assert(l1 == l2, s"$qsId2sgIdMapping != $mapping2")

    val qsMap2 = qsMap.map(x =>
      (
        x._1.toString,
        x._2.json ~
          ("RelevantStageIds" -> sgMap(
            qsId2sgIdMapping(x._1)
          ).stageIds.toList) ~
          ("DurationInMs" -> qsId2QSResultTimes(x._1).DurationInMs) ~
          ("TotalTaskDurationInMs" ->
            qsId2QSResultTimes(x._1).totalTasksDurationInMs) ~
          ("RelevantStageIds" ->
            qsId2QSResultTimes(x._1).relevantStageIds.toList)
      )
    )

    val json = ("RuntimeLQPs" -> lqpMap2.toMap) ~
      ("RuntimeQSs" -> qsMap2.toMap) ~
      ("SQLStartTimeInMs" -> sqlStartTimeInMs) ~
      ("SQLEndTimeInMs" -> sqlEndTimeInMs) ~
      ("SQLDurationInMs" -> (sqlEndTimeInMs - sqlStartTimeInMs))
    pretty(render(json))
  }

}
