package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.{
  SGResults,
  IOBytesUnit,
  SGUnit
}
import org.apache.spark.scheduler.{
  SparkListenerJobStart,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd
}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class RuntimeSparkStageGroupCollector(verbose: Boolean = true) {

  private val stageGroupMap: TrieMap[Int, SGUnit] = new TrieMap()
  private val stageGroupSign2Id = new TrieMap[String, Int]()

  val stageTotalTasksDurationDict: TrieMap[Int, Long] = new TrieMap()
  val stageStartEndTimeDict: TrieMap[Int, (Long, Long)] = new TrieMap()
  val stageIOBytesDict: TrieMap[Int, IOBytesUnit] = new TrieMap()

  private val listLeafStageIds: mutable.Set[Int] = mutable.Set[Int]()

  def getStageGroupMap: TrieMap[Int, SGUnit] = stageGroupMap

  private def addStage(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val rddInfos = stageSubmitted.stageInfo.rddInfos
    val stageId = stageSubmitted.stageInfo.stageId
    val stageGroupSign =
      rddInfos.map(_.scope.get.id.toInt).distinct.sorted.mkString(",")
    val table = rddInfos
      .filter(_.name == "FileScanRDD")
      .map(_.scope.get.name.split('.').last)
      .sorted
      .mkString(",")

    if (stageGroupSign2Id.contains(stageGroupSign)) {
      val sgId = stageGroupSign2Id(stageGroupSign)
      assert(stageGroupMap.contains(sgId) && stageGroupMap(sgId).table == table)
      stageGroupMap(sgId).stageIds = stageGroupMap(sgId).stageIds :+ stageId
    } else {
      val sgId = stageGroupSign2Id.size
      stageGroupSign2Id += (stageGroupSign -> sgId)
      stageGroupMap += (sgId ->
        SGUnit(sgId, stageGroupSign, Seq(stageId), table))
    }

    if (verbose) {
      println(
        s"stageId=${stageId} is added to stageGroup=${stageGroupMap(stageGroupSign2Id(stageGroupSign))}"
      )

    }
  }

  def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val desc =
      jobStart.properties.getProperty("spark.job.description")
    if (desc != null && desc.contains("Listing leaf files and directories")) {
      listLeafStageIds ++= jobStart.stageIds
    }
  }

  def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted
  ): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    if (!listLeafStageIds.contains(stageId)) {
      stageStartEndTimeDict.update(
        stageId,
        (
          stageCompleted.stageInfo.submissionTime.get,
          stageCompleted.stageInfo.completionTime.get
        )
      )
      stageIOBytesDict.update(
        stageId,
        IOBytesUnit(
          inputRead =
            stageCompleted.stageInfo.taskMetrics.inputMetrics.bytesRead,
          inputWritten =
            stageCompleted.stageInfo.taskMetrics.outputMetrics.bytesWritten,
          shuffleRead =
            stageCompleted.stageInfo.taskMetrics.shuffleReadMetrics.totalBytesRead,
          shuffleWritten =
            stageCompleted.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten
        )
      )
    }
  }

  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    if (!listLeafStageIds.contains(stageId))
      addStage(stageSubmitted)
  }

  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageId = taskEnd.stageId
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      stageTotalTasksDurationDict.get(stageId) match {
        case Some(v) =>
          stageTotalTasksDurationDict.update(stageId, v + info.duration)
        case None =>
          stageTotalTasksDurationDict.update(stageId, info.duration)
      }
    }
  }

  def aggregateAll(): IOBytesUnit = {
    stageGroupMap.values.flatMap(_.stageIds).map(stageIOBytesDict(_)).reduce {
      (x, y) =>
        IOBytesUnit(
          inputRead = x.inputRead + y.inputRead,
          inputWritten = x.inputWritten + y.inputWritten,
          shuffleRead = x.shuffleRead + y.shuffleRead,
          shuffleWritten = x.shuffleWritten + y.shuffleWritten
        )
    }
  }

  def aggregateResults: TrieMap[Int, SGResults] = {
    stageGroupMap.map { case (sgId, sgUnit) =>
      val stageIds = sgUnit.stageIds
      val (startTime, endTime) =
        stageIds.map(stageStartEndTimeDict(_)).reduce { (x, y) =>
          (Math.min(x._1, y._1), Math.max(x._2, y._2))
        }
      val totalTasksDurationInMs =
        stageIds.map(stageTotalTasksDurationDict(_)).sum
      val ioBytesAggregated = stageIds
        .map(stageIOBytesDict(_))
        .reduce { (x, y) =>
          IOBytesUnit(
            inputRead = x.inputRead + y.inputRead,
            inputWritten = x.inputWritten + y.inputWritten,
            shuffleRead = x.shuffleRead + y.shuffleRead,
            shuffleWritten = x.shuffleWritten + y.shuffleWritten
          )
        }

      sgId -> SGResults(
        id = sgId,
        durationInMs = endTime - startTime,
        totalTasksDurationInMs = totalTasksDurationInMs,
        relevantStages = sgUnit.stageIds,
        ioBytes = ioBytesAggregated
      )
    }
  }

}
