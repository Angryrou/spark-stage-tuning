package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.F.KnobKV
import edu.polytechnique.cedar.spark.sql.component._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.ui.SparkListenerOnQueryStageSubmitted

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class RuntimeQueryStageCollector(verbose: Boolean = false) {
  private val qsOptId = new AtomicInteger(0)
  private val qsOptIdMap: TrieMap[Int, QSMetrics] = new TrieMap()
  private val snapshotMap = new TrieMap[Int, RunningSnapshot]()
  private val thetaRMap = new TrieMap[Int, Map[String, Array[KnobKV]]]()
  private val tableMap = new TrieMap[Int, String]()

  private val qsIndexMap = new TrieMap[(Int, Int), QSIndex]()
  private var qsMap: Option[Map[Int, QSUnit]] = None

  def getQsOptId: Int = qsOptId.get()

  def exposeQueryStageForOptimization(
      plan: SparkPlan,
      spark: SparkSession,
      observedLogicalQS: Set[LogicalPlan],
      snapshot: RunningSnapshot
  ): Int = {
    val curId = qsOptId.getAndIncrement()
    qsOptIdMap += (curId -> F.exposeQSMetrics(plan, observedLogicalQS))
    snapshotMap += (curId -> snapshot)
    thetaRMap += (curId -> F.getRuntimeConfiguration(spark))
    tableMap += (curId -> F.getLeafTables(plan))
    curId
  }

  def onQueryStageSubmitted(e: SparkListenerOnQueryStageSubmitted): Unit = {
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
        assert(!qsIndexMap.contains((e.waveId, e.idInWave)))
        qsIndexMap.update((e.waveId, e.idInWave), qsIndex)
        if (verbose) println(s"addQSIndex: $qsIndex")
      case None =>
        if (verbose)
          println(
            s"skip: (wave:${e.waveId},${e.idInWave}) in plan.${e.planId}->QS.${e.qsId}; mostly due to reuse Exchange."
          )
    }
  }

  private def addToQueue(
      mainMetaMap: mutable.TreeMap[(Int, Int), QSIndex],
      subMetaMapGroups: mutable.Map[Int, mutable.TreeMap[(Int, Int), QSIndex]],
      subqueryFinished: mutable.Map[Int, Boolean]
  ): mutable.ArrayBuffer[QSIndex] = {
    val qsExecutionQueue = mutable.ArrayBuffer[QSIndex]()
    mainMetaMap.foreach { case ((waveId, idInWave), qsIndex) =>
      assert(qsIndex.waveId == waveId && qsIndex.idInWave == idInWave)
      if (qsIndex.hasSubqueries) {
        qsIndex.subqueryIds.foreach { subId =>
          if (subqueryFinished.contains(subId) && !subqueryFinished(subId)) {
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

  private def getQueryStageExecutionQueue: mutable.ArrayBuffer[QSIndex] = {
    val subqueryFinished = mutable.Map[Int, Boolean]()
    val mainIndexMap = mutable.TreeMap[(Int, Int), QSIndex]()
    val subIndexMapGroup =
      mutable.Map[Int, mutable.TreeMap[(Int, Int), QSIndex]]()
    qsIndexMap.toSeq.sortBy(_._1).foreach {
      case ((waveId, idInWave), qsIndex) =>
        if (qsIndex.isSubquery) {
          if (!subIndexMapGroup.contains(qsIndex.planId)) {
            subIndexMapGroup += (qsIndex.planId ->
              mutable.TreeMap[(Int, Int), QSIndex]())
          }
          subIndexMapGroup(qsIndex.planId) += ((waveId, idInWave) -> qsIndex)
          subqueryFinished += (qsIndex.planId -> false)
        } else {
          mainIndexMap += ((waveId, idInWave) -> qsIndex)
        }
    }
    addToQueue(mainIndexMap, subIndexMapGroup, subqueryFinished)
  }

  private def getMappingToSG(
      qsExecutionQueue: mutable.ArrayBuffer[QSIndex],
      stageGroupMap: TrieMap[Int, SGUnit]
  ): Map[Int, Int] = {
    val stageGroupTableMap = stageGroupMap.values
      .map(x => (x.id, x.table))
      .groupBy(_._2)
      .mapValues(_.map(_._1).toSeq.sorted)
    val qsId2sgIdMapping = mutable.Map[Int, Int]()
    stageGroupTableMap.foreach { case (table, sgIdList) =>
      qsExecutionQueue.zipWithIndex
        .filter(x => tableMap(x._1.optimizedStageOrder) == table)
        .map(_._2)
        .zip(sgIdList)
        .foreach { case (qsId, sgId) =>
          qsId2sgIdMapping += (qsId -> sgId)
        }
    }
    qsId2sgIdMapping.toMap
  }

  def getQueryStageMap(
                        stageGroupMap: TrieMap[Int, SGUnit],
                        stageGroupResultsMap: TrieMap[Int, SGResults]
  ): Map[Int, QSUnit] = {
    assert(stageGroupMap.size == qsIndexMap.size)
    val qsExecutionQueue = getQueryStageExecutionQueue
    val qsId2sgIdMapping = getMappingToSG(qsExecutionQueue, stageGroupMap)
    qsMap = Some(qsExecutionQueue.zipWithIndex.map { case (qsIndex, index) =>
      val tmpOptId = qsIndex.optimizedStageOrder
      val sgId = qsId2sgIdMapping(index)
      assert(stageGroupMap(sgId).table == tableMap(tmpOptId))
      index -> QSUnit(
        id = index,
        qsOptId = tmpOptId,
        qsUnitMetrics = qsOptIdMap(tmpOptId),
        ioBytes = stageGroupResultsMap(sgId).ioBytes,
        durationInMs = stageGroupResultsMap(sgId).durationInMs,
        totalTasksDurationInMs =
          stageGroupResultsMap(sgId).totalTasksDurationInMs,
        snapshot = snapshotMap(tmpOptId),
        thetaR = thetaRMap(tmpOptId),
        relevantStages = stageGroupResultsMap(sgId).relevantStages,
        table = tableMap(tmpOptId)
      )
    }.toMap)
    qsMap.get
  }

}
