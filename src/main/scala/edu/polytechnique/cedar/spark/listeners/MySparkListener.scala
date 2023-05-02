package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.AggMetrics
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerEvent,
  SparkListenerStageCompleted,
  SparkListenerTaskEnd
}
import org.apache.spark.sql.execution.ui.{
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart
}
import org.apache.spark.Success

case class MySparkListener(aggMetrics: AggMetrics) extends SparkListener {

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (aggMetrics.successFlag) {
      val stageId = taskEnd.stageId
      if (taskEnd.reason == Success) {
        aggMetrics.stageFirstTaskTime.get(stageId) match {
          case Some(ft) =>
            aggMetrics.stageFirstTaskTime.update(
              stageId,
              taskEnd.taskInfo.launchTime.min(ft)
            )
          case None =>
            aggMetrics.stageFirstTaskTime += (stageId -> taskEnd.taskInfo.launchTime)
        }
        aggMetrics.stageTotalTaskTime.get(stageId) match {
          case Some(dt) =>
            aggMetrics.stageTotalTaskTime.update(
              stageId,
              taskEnd.taskInfo.duration + dt
            )
          case None =>
            aggMetrics.stageTotalTaskTime += (stageId -> taskEnd.taskInfo.duration)
        }
      } else {
        aggMetrics.successFlag = false
        aggMetrics.stageFirstTaskTime += (stageId -> -1L)
        aggMetrics.stageTotalTaskTime += (stageId -> -1L)
      }
    }
    super.onTaskEnd(taskEnd)
  }

  override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted
  ): Unit = {
    if (aggMetrics.successFlag) {
      val stageInfo = stageCompleted.stageInfo
      val stageId = stageInfo.stageId
      assert(!aggMetrics.stageSubmittedTime.contains(stageId))
      if (stageInfo.failureReason.isEmpty) {
        val stageSubmittedTime = stageInfo.submissionTime.getOrElse(-1L)
        val stageCompletedTime = stageInfo.completionTime.getOrElse(-1L)
        assert(stageSubmittedTime > 0L && stageCompletedTime > 0L)
        aggMetrics.stageSubmittedTime += (stageId -> stageSubmittedTime)
        aggMetrics.stageCompletedTime += (stageId -> stageCompletedTime)
      } else {
        aggMetrics.successFlag = false
        aggMetrics.stageSubmittedTime += (stageId -> -1L)
        aggMetrics.stageCompletedTime += (stageId -> -1L)
      }
    }
    super.onStageCompleted(stageCompleted)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {

    event match {

      case e: SparkListenerSQLExecutionStart =>
        if (aggMetrics.successFlag) {
          assert(
            !aggMetrics.initialPlanTimeMetric.queryStartTimeMap.contains(
              e.executionId
            )
          )
          aggMetrics.initialPlanTimeMetric.queryStartTimeMap += (e.executionId -> e.time)
          println(s"${e.executionId}, start ${e.time}")
        }

      case e: SparkListenerSQLExecutionEnd =>
        if (e.errorMessage.isDefined && e.errorMessage.get.nonEmpty) {
          aggMetrics.successFlag = false
        }
        if (aggMetrics.successFlag) {
          assert(
            !aggMetrics.initialPlanTimeMetric.queryEndTimeMap.contains(
              e.executionId
            )
          )
          aggMetrics.initialPlanTimeMetric.queryEndTimeMap += (e.executionId -> e.time)
          println(s"${e.executionId}, end ${e.time}")
        }

      case _ =>
    }
    super.onOtherEvent(event)
  }

}
