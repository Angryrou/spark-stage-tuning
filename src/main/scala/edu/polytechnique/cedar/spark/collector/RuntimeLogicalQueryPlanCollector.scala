package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.F.KnobKV
import edu.polytechnique.cedar.spark.sql.component.{
  F,
  IOBytesUnit,
  LQPUnit,
  RunningSnapshot
}
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
class RuntimeLogicalQueryPlanCollector {

  private val lqpId = new AtomicInteger(1)
  private val lqpUnitMap = new TrieMap[Int, LQPUnit]()
  private val startTimeInMsMap = new TrieMap[Int, Long]()
  private val snapshotMap = new TrieMap[Int, RunningSnapshot]()
  private val thetaRMap = new TrieMap[Int, Map[String, Array[KnobKV]]]()
  private val finishedStageIdsMap = new TrieMap[Int, Set[Int]]()
  def getLqpId: Int = lqpId.get()

  def exportRuntimeLogicalPlanBeforeOptimization(
      lqpUnit: LQPUnit,
      startTimeInMs: Long,
      snapshot: RunningSnapshot,
      runtimeKnobsDict: Map[String, Array[KnobKV]],
      finishedStageIds: Set[Int]
  ): Int = {
    val curId = lqpId.getAndIncrement()
    lqpUnitMap += (curId -> lqpUnit)
    startTimeInMsMap += (curId -> startTimeInMs)
    snapshotMap += (curId -> snapshot)
    thetaRMap += (curId -> runtimeKnobsDict)
    finishedStageIdsMap += (curId -> finishedStageIds)
    curId
  }

  def exportMap(
      sqlEndTimeInMs: Long,
      stageIOBytesDict: TrieMap[Int, IOBytesUnit]
  ): Map[String, JsonAST.JObject] =
    lqpUnitMap.map { x =>
      val remainingStageIds =
        stageIOBytesDict.keySet -- finishedStageIdsMap(x._1)
      (
        x._1.toString,
        x._2.json ~
          ("RunningQueryStageSnapshot" -> snapshotMap(x._1).toJson) ~
          ("StartTimeInMs" -> startTimeInMsMap(x._1)) ~
          ("RuntimeConfiguration" -> thetaRMap(x._1)
            .map(y => (y._1, y._2.toSeq))) ~
          ("Objectives" -> (
            ("DurationInMs" -> (sqlEndTimeInMs - startTimeInMsMap(x._1))) ~
              ("IOBytes" -> F
                .aggregateIOBytes(remainingStageIds.toSeq, stageIOBytesDict)
                .json)
          ))
      )
    }.toMap
}
