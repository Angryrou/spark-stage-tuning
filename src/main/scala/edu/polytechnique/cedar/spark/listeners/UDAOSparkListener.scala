package edu.polytechnique.cedar.spark.listeners

import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
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

case class UDAOSparkListener(
    rc: RuntimeCollector,
    debug: Boolean,
    verbose: Boolean = true
) extends SparkListener {

  val listLeafStageIds: mutable.Set[Int] = mutable.Set[Int]()

  override def onJobStart(
      jobStart: SparkListenerJobStart
  ): Unit = {
    val desc =
      jobStart.properties.getProperty("spark.job.description")
    if (desc != null && desc.contains("Listing leaf files and directories")) {
      listLeafStageIds ++= jobStart.stageIds
    }
  }

  override def onStageSubmitted(
      stageSubmitted: SparkListenerStageSubmitted
  ): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    val numTasks = stageSubmitted.stageInfo.numTasks
    rc.runtimeSnapshotTracker.numTasksBookKeeper.update(stageId, numTasks)

    if (listLeafStageIds.contains(stageId)) {
      if (debug) {
        println(
          s"bypass stageId=${stageId} because it is a list-leaf-files-and-directories stage"
        )
      }
    } else {
      rc.runtimeDependencyTracker.addStage(stageSubmitted)
      // verbose for the debug purpose
      if (verbose) {
        println(
          s"stageId=${stageId}, taskNum:$numTasks, ${rc.runtimeDependencyTracker.stageMap(stageId)}"
        )
      }
    }
  }

  override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted
  ): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    if (listLeafStageIds.contains(stageId)) {
      if (debug) {
        println(
          s"bypass stageId=${stageId} because it is a list-leaf-files-and-directories stage"
        )
      }
    } else {
      rc.runtimeDependencyTracker.stageStartEndTimeDict
        .update(
          stageId,
          (
            stageCompleted.stageInfo.submissionTime.get,
            stageCompleted.stageInfo.completionTime.get
          )
        )
      // todo: add IO metrics for each stage

    }
  }

  override def onTaskStart(
      taskStart: SparkListenerTaskStart
  ): Unit = {
    val stageId = taskStart.stageId
    val info = taskStart.taskInfo
    if (info != null) {
      rc.runtimeSnapshotTracker.startedTasksNumTracker.get(stageId) match {
        case Some(v) =>
          rc.runtimeSnapshotTracker.startedTasksNumTracker
            .update(stageId, v + 1)
        case None =>
          rc.runtimeSnapshotTracker.startedTasksNumTracker.update(stageId, 1)
      }
    }

  }
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageId = taskEnd.stageId
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      rc.runtimeSnapshotTracker.taskMetricsMap.getOrElseUpdate(
        stageId,
        mutable.Buffer[(TaskInfo, TaskMetrics)]()
      ) += ((info, metrics))
      rc.runtimeDependencyTracker.stageTotalTasksDurationDict.get(
        stageId
      ) match {
        case Some(v) =>
          rc.runtimeDependencyTracker.stageTotalTasksDurationDict
            .update(stageId, v + info.duration)
        case None =>
          rc.runtimeDependencyTracker.stageTotalTasksDurationDict
            .update(stageId, info.duration)
      }
    }
    if (
      rc.runtimeSnapshotTracker.taskMetricsMap(stageId).size ==
        rc.runtimeSnapshotTracker.numTasksBookKeeper(stageId)
    )
      rc.runtimeSnapshotTracker.removeStageById(stageId)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {

    event match {

//      case e: SparkListenerSQLAllChildMaterialized =>
//        assert(
//          e.executionId == 1,
//          "Assertion failed: we should not have executionId != 1 using AQE"
//        )
//        rc.flushQSs(
//          planId = e.planId,
//          isSubquery = e.isSubquery,
//          recordedStages = e.stagesToReplace,
//          aqeContext = e.context
//        )

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
