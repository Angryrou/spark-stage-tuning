package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerEvent,
  SparkListenerStageCompleted,
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
    }
  }

  override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted
  ): Unit = rc.runtimeStageTaskTracker.removeStage(stageCompleted)

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
