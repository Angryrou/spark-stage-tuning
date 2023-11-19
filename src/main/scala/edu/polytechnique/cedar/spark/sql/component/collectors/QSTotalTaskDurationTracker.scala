package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{QSResultTimes, QSUnit}

import scala.collection.mutable

class QSTotalTaskDurationTracker {
  val stageTotalTasksDurationDict: mutable.Map[Int, Long] =
    mutable.Map[Int, Long]()
  val stageStartEndTimeDict: mutable.Map[Int, (Long, Long)] =
    mutable.Map[Int, (Long, Long)]()
  val rootRddId2StageIds: mutable.Map[Int, Array[Int]] =
    mutable.Map[Int, Array[Int]]()
  val listLeafStageIds: mutable.Set[Int] = mutable.Set[Int]()

  private def getRootRddId2QSResultTimes: mutable.Map[Int, QSResultTimes] = {
    val rootRddId2QSResultTimes = mutable.TreeMap[Int, QSResultTimes]()
    for ((rootRddIds, stageIds) <- rootRddId2StageIds) {
      val startTime = stageIds.map(stageStartEndTimeDict(_)._1).min
      val endTime = stageIds.map(stageStartEndTimeDict(_)._2).max
      val durationInMs = endTime - startTime
      val totalTasksDurationInMs =
        stageIds.map(stageTotalTasksDurationDict(_)).sum
      val qSResultTimes =
        QSResultTimes(durationInMs, totalTasksDurationInMs, stageIds)
      rootRddId2QSResultTimes += (rootRddIds -> qSResultTimes)
    }
    rootRddId2QSResultTimes
  }

  def getQsId2QSResultTimes(
      qsMap: mutable.Map[Int, QSUnit]
  ): Map[Int, QSResultTimes] = {
    val rootRddId2QSResultTimes = getRootRddId2QSResultTimes
    assert(qsMap.size == rootRddId2QSResultTimes.size)

    val qsIdSeq = qsMap.keys.toSeq.sorted
    val rootRddIdSeq = rootRddId2QSResultTimes.keys.toSeq.sorted
    val qsId2RootRddId = qsIdSeq.zip(rootRddIdSeq).toMap

    qsId2RootRddId.map(x => (x._1, rootRddId2QSResultTimes(x._2)))
  }

}
