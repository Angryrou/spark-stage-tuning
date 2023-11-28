package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.SparkContext
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerEvent,
  SparkListenerJobStart,
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
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class UDAOSparkListener(rc: RuntimeCollector, debug: Boolean)
    extends SparkListener {

  implicit val formats: DefaultFormats.type = DefaultFormats

  override def onJobStart(
      jobStart: SparkListenerJobStart
  ): Unit = {
    val desc =
      jobStart.properties.getProperty("spark.job.description")
    if (desc != null && desc.contains("Listing leaf files and directories")) {
      rc.qsTotalTaskDurationTracker.listLeafStageIds ++= jobStart.stageIds
    } else {
      val json = parse(jobStart.properties.getProperty("spark.rdd.scope"))
      val scopeId = (json \ "id").asInstanceOf[JString].s.toInt
      val stageIds = jobStart.stageIds.toArray[Int]
      // get scopeId to [stageId] mapping
      rc.qsTotalTaskDurationTracker.scopeId2StageIds.get(scopeId) match {
        case Some(v) =>
          rc.qsTotalTaskDurationTracker.scopeId2StageIds
            .update(scopeId, v ++ stageIds)
        case None =>
          rc.qsTotalTaskDurationTracker.scopeId2StageIds
            .update(scopeId, stageIds)
      }
      println(
        s"jobId: ${jobStart.jobId}, scopeId: ${scopeId}, stages: ${stageIds.mkString(",")}"
      )
    }
  }

  override def onStageSubmitted(
      stageSubmitted: SparkListenerStageSubmitted
  ): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    val numTasks = stageSubmitted.stageInfo.numTasks
    rc.runtimeStageTaskTracker.numTasksBookKeeper.update(stageId, numTasks)

    val rddIds = stageSubmitted.stageInfo.rddInfos.map(_.id).sorted
    val scanTables = stageSubmitted.stageInfo.rddInfos
      .filter(_.name == "FileScanRDD")
      .map(_.scope.get.name.split('.').last)
      .mkString(",")
    val fileScopeIds = stageSubmitted.stageInfo.rddInfos
      .filter(_.name == "FileScanRDD")
      .map(_.scope.get.id)
      .mkString(",")
    println(
      s"stageId=${stageId}, taskNum:$numTasks, rddIds:${rddIds
        .mkString(",")}, relations:${scanTables}, fileScopeIds:${fileScopeIds}"
    )
  }

  override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted
  ): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    if (rc.qsTotalTaskDurationTracker.listLeafStageIds.contains(stageId)) {
      if (debug) {
        println(
          s"bypass stageId=${stageId} because it is a list-leaf-files-and-directories stage"
        )
      }
    } else {
      rc.qsTotalTaskDurationTracker.stageStartEndTimeDict
        .update(
          stageId,
          (
            stageCompleted.stageInfo.submissionTime.get,
            stageCompleted.stageInfo.completionTime.get
          )
        )
    }
  }

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
      rc.runtimeStageTaskTracker.taskMetricsMap(stageId).size ==
        rc.runtimeStageTaskTracker.numTasksBookKeeper(stageId)
    )
      rc.runtimeStageTaskTracker.removeStageById(stageId)
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
