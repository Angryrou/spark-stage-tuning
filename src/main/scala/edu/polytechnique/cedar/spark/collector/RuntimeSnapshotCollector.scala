package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.RunningSnapshot
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart, TaskInfo}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class RuntimeSnapshotCollector {

  val numTasksBookKeeper: TrieMap[Int, Int] = new TrieMap()
  private val startedTasksNumTracker: TrieMap[Int, Int] = new TrieMap()
  private val taskMetricsMap
      : TrieMap[Int, mutable.Buffer[(TaskInfo, TaskMetrics)]] =
    new TrieMap()
  private val defaultProbabilities = Array(0, 0.25, 0.5, 0.75, 1.0)

  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    val numTasks = stageSubmitted.stageInfo.numTasks
    numTasksBookKeeper.update(stageId, numTasks)
  }

  def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val stageId = taskStart.stageId
    val info = taskStart.taskInfo
    if (info != null) {
      startedTasksNumTracker.get(stageId) match {
        case Some(v) =>
          startedTasksNumTracker.update(stageId, v + 1)
        case None =>
          startedTasksNumTracker.update(stageId, 1)
      }
    }
  }

  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageId = taskEnd.stageId
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      taskMetricsMap.getOrElseUpdate(
        stageId,
        mutable.Buffer[(TaskInfo, TaskMetrics)]()
      ) += ((info, metrics))
    }
    if (taskMetricsMap(stageId).size == numTasksBookKeeper(stageId))
      removeStageById(stageId)
  }

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
  def snapshot(): RunningSnapshot = {
    RunningSnapshot(
      getRuntimeStageRunningTasksNum,
      getRuntimeStageFinishedTasksNum,
      getRuntimeStageFinishedTasksTotalTimeInMs,
      getRuntimeStageFinishedTasksDistributionInMs
    )
  }
}
