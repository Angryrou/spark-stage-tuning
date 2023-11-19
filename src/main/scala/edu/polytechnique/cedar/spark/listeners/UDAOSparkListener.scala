package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerEvent,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd,
  SparkListenerTaskStart,
  TaskInfo
}
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart
}

import scala.collection.mutable

case class UDAOSparkListener(rc: RuntimeCollector, debug: Boolean)
    extends SparkListener {

  override def onTaskStart(
      taskStart: SparkListenerTaskStart
  ): Unit = {
    val stageId = taskStart.stageId
    val info = taskStart.taskInfo
    if (info != null) {
      rc.runtimeStageTaskTracker.startedTasksNumTracker.get(stageId) match {
        case Some(v) =>
          rc.runtimeStageTaskTracker.startedTasksNumTracker
            .update(stageId, v + 1)
        case None =>
          rc.runtimeStageTaskTracker.startedTasksNumTracker.update(stageId, 1)
      }
    }

  }
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageId = taskEnd.stageId
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      rc.runtimeStageTaskTracker.taskMetricsMap.getOrElseUpdate(
        stageId,
        mutable.Buffer[(TaskInfo, TaskMetrics)]()
      ) += ((info, metrics))
      rc.qsTotalTaskDurationTracker.stageTotalTasksDurationDict.get(
        stageId
      ) match {
        case Some(v) =>
          rc.qsTotalTaskDurationTracker.stageTotalTasksDurationDict
            .update(stageId, v + info.duration)
        case None =>
          rc.qsTotalTaskDurationTracker.stageTotalTasksDurationDict
            .update(stageId, info.duration)
      }
    }
    if (
      rc.runtimeStageTaskTracker.taskMetricsMap.size ==
        rc.runtimeStageTaskTracker.numTasksBookKeeper(stageId)
    )
      rc.runtimeStageTaskTracker.removeStageById(stageId)
  }

  override def onStageSubmitted(
      stageSubmitted: SparkListenerStageSubmitted
  ): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    val numTasks = stageSubmitted.stageInfo.numTasks
    rc.runtimeStageTaskTracker.numTasksBookKeeper.update(stageId, numTasks)
  }

  override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted
  ): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val minRddId = stageCompleted.stageInfo.rddInfos.map(_.id).min
    val rootRdd =
      stageCompleted.stageInfo.rddInfos.filter(_.id == minRddId).head.id
    rc.qsTotalTaskDurationTracker.rootRddId2StageIds.get(rootRdd) match {
      case Some(v) =>
        rc.qsTotalTaskDurationTracker.rootRddId2StageIds
          .update(rootRdd, v :+ stageId)
      case None =>
        rc.qsTotalTaskDurationTracker.rootRddId2StageIds
          .update(rootRdd, Array(stageId))
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {

    event match {

      case e: SparkListenerSQLExecutionStart =>
        assert(
          e.executionId == 1 || e.executionId == 0,
          "Assertion failed: we should not have executionId.isEmpty or executionId > 2"
        )
        if (e.executionId == 1) {
          rc.setSQLStartTimeInMs(e.time)
          if (debug) {
            println(
              s"added SQLStartTimeInMs ${e.time} for execId=${e.executionId}"
            )
          }
        } else {
          if (debug) {
            println(
              s"bypass SQLStartTimeInMs ${e.time} for execId=${e.executionId}"
            )
          }
        }

      case e: SparkListenerSQLExecutionEnd =>
        assert(
          e.executionId == 1 || e.executionId == 0,
          "Assertion failed: we should not have executionId.isEmpty or executionId > 2"
        )
        assert(
          e.errorMessage.isEmpty || e.errorMessage.get.isEmpty,
          e.errorMessage
        )
        if (e.executionId == 1) {
          rc.setSQLEndTimeInMs(e.time)
          if (debug) {
            println(
              s"added SQLEndTimeInMs ${e.time} for execId=${e.executionId}"
            )
          }
        } else {
          if (debug) {
            println(
              s"bypass SQLEndTimeInMs ${e.time} for execId=${e.executionId}"
            )
          }
        }

      case _ =>
    }
    super.onOtherEvent(event)
  }

}
