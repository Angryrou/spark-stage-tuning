package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.RunningQueryStageSnapshot
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class RuntimeSnapshotTracker {

  val numTasksBookKeeper: TrieMap[Int, Int] = new TrieMap()
  val startedTasksNumTracker: TrieMap[Int, Int] = new TrieMap()
  val taskMetricsMap: TrieMap[Int, mutable.Buffer[(TaskInfo, TaskMetrics)]] =
    new TrieMap()
  private val defaultProbabilities = Array(0, 0.25, 0.5, 0.75, 1.0)

  def removeStageById(stageId: Int): Unit = {
    assert(
      startedTasksNumTracker.contains(stageId) &&
        taskMetricsMap.contains(stageId) &&
        numTasksBookKeeper.contains(stageId)
    )
    startedTasksNumTracker -= stageId
    taskMetricsMap -= stageId
    numTasksBookKeeper -= stageId
  }

  private def getRuntimeStageFinishedTasksNum =
    taskMetricsMap.values.map(_.size).sum

  private def getRuntimeStageStartedTasksNum = startedTasksNumTracker.values.sum

  private def getRuntimeStageRunningTasksNum =
    getRuntimeStageStartedTasksNum - getRuntimeStageFinishedTasksNum

  def getRuntimeStageFinishedTasksTotalTimeInMs: Double = {
    val durationInMsList =
      taskMetricsMap.values.flatten.map(_._1.duration.toDouble)
    durationInMsList.sum
  }

  private def closestIndex(
      p: Double,
      startIdx: Int,
      endIdx: Int,
      length: Int
  ) = {
    math.min((p * length).toInt + startIdx, endIdx - 1)
  }
  private def getRuntimeStageFinishedTasksDistributionInMs = {
    val durationInMsList =
      taskMetricsMap.values.flatten.map(_._1.duration.toDouble).toSeq.sorted
    val startIdx = 0
    val endIdx = durationInMsList.size
    if (endIdx > startIdx)
      defaultProbabilities.toIndexedSeq.map { p: Double =>
        durationInMsList(closestIndex(p, startIdx, endIdx, endIdx - startIdx))
      }
    else
      defaultProbabilities.toIndexedSeq.map(_ => 0.0)
  }
  def snapshot(): RunningQueryStageSnapshot = {
    RunningQueryStageSnapshot(
      getRuntimeStageRunningTasksNum,
      getRuntimeStageFinishedTasksNum,
      getRuntimeStageFinishedTasksTotalTimeInMs,
      getRuntimeStageFinishedTasksDistributionInMs
    )
  }
}