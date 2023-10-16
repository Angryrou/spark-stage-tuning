package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.RunningQueryStageSnapshot
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerStageCompleted, TaskInfo}

import scala.collection.mutable

class RuntimeStageTaskTracker {

  val startedTasksNumTracker: mutable.Map[Int, Int] = mutable.Map()
  val taskMetricsMap
      : mutable.Map[Int, mutable.Buffer[(TaskInfo, TaskMetrics)]] =
    mutable.Map()
  val defaultProbabilities = Array(0, 0.25, 0.5, 0.75, 1.0)

  def removeStage(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    assert(
      startedTasksNumTracker.contains(stageId) &&
        taskMetricsMap.contains(stageId)
    )
    startedTasksNumTracker -= stageId
    taskMetricsMap -= stageId
  }

  def getRuntimeStageFinishedTasksNum = taskMetricsMap.values.map(_.size).sum

  def getRuntimeStageStartedTasksNum = startedTasksNumTracker.values.sum

  def getRuntimeStageRunningTasksNum =
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
  def getRuntimeStageFinishedTasksDistributionInMs = {
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
  def snapshot() = {
    RunningQueryStageSnapshot(
      getRuntimeStageRunningTasksNum,
      getRuntimeStageFinishedTasksNum,
      getRuntimeStageFinishedTasksTotalTimeInMs,
      getRuntimeStageFinishedTasksDistributionInMs
    )
  }
}
