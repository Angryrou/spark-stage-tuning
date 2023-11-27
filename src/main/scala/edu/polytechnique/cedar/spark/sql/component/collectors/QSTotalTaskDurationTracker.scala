package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{QSResultTimes, QSUnit}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class QSTotalTaskDurationTracker {
  val stageTotalTasksDurationDict: TrieMap[Int, Long] = new TrieMap[Int, Long]()
  val stageStartEndTimeDict: TrieMap[Int, (Long, Long)] =
    new TrieMap[Int, (Long, Long)]()
  val scopeId2StageIds: TrieMap[Int, Array[Int]] =
    new TrieMap[Int, Array[Int]]()
  val listLeafStageIds: mutable.Set[Int] = mutable.Set[Int]()

  private def getScopeId2QSResultTimes: mutable.Map[Int, QSResultTimes] = {
    val scopeId2QSResultTimes = new mutable.TreeMap[Int, QSResultTimes]()

    for ((scopeId, stageIdsAll) <- scopeId2StageIds) {
      val stageIds = stageIdsAll.filter(stageStartEndTimeDict.contains)
      val startTime = stageIds.map(stageStartEndTimeDict(_)._1).min
      val endTime = stageIds.map(stageStartEndTimeDict(_)._2).max
      val durationInMs = endTime - startTime
      val totalTasksDurationInMs =
        stageIds.map(stageTotalTasksDurationDict(_)).sum
      val qSResultTimes =
        QSResultTimes(durationInMs, totalTasksDurationInMs, stageIds)
      scopeId2QSResultTimes += (scopeId -> qSResultTimes)
    }
    scopeId2QSResultTimes
  }

  def getQsId2QSResultTimes(
      qsMap: TrieMap[Int, QSUnit]
  ): Map[Int, QSResultTimes] = {
    val scopeId2QSResultTimes = getScopeId2QSResultTimes
    assert(qsMap.size == scopeId2QSResultTimes.size)

    val qsIdSeq = qsMap.keys.toSeq.sorted
    val rootRddIdSeq = scopeId2QSResultTimes.keys.toSeq.sorted
    val qsId2RootRddId = qsIdSeq.zip(rootRddIdSeq).toMap

    qsId2RootRddId.map(x => (x._1, scopeId2QSResultTimes(x._2)))
  }

}
