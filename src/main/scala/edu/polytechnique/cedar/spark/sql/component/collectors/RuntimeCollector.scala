package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{
  LQPUnit,
  QSUnit,
  RunningQueryStageSnapshot
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class RuntimeCollector() {

  private val lqpId: AtomicInteger = new AtomicInteger(1)
  private val lqpMap: mutable.Map[Int, LQPUnit] =
    mutable.TreeMap[Int, LQPUnit]()
  private val lqpStartTimeInMsMap: mutable.Map[Int, Long] =
    mutable.TreeMap[Int, Long]()
  private val lqpSnapshot: mutable.Map[Int, RunningQueryStageSnapshot] =
    mutable.TreeMap[Int, RunningQueryStageSnapshot]()
  private val lqpThetaR
      : mutable.Map[Int, Map[String, Array[(String, String)]]] =
    mutable.TreeMap[Int, Map[String, Array[(String, String)]]]()

  private var sqlStartTimeInMs: Long = -1
  private var sqlEndTimeInMs: Long = -1

  private val qsId: AtomicInteger = new AtomicInteger(0)
  private val qsMap: mutable.Map[Int, QSUnit] = mutable.TreeMap[Int, QSUnit]()
  private val qsStartTimeMap: mutable.Map[Int, Long] =
    mutable.TreeMap[Int, Long]()
  private val qsSnapshot: mutable.Map[Int, RunningQueryStageSnapshot] =
    mutable.TreeMap[Int, RunningQueryStageSnapshot]()
  private val qsThetaR: mutable.Map[Int, Map[String, Array[(String, String)]]] =
    mutable.TreeMap[Int, Map[String, Array[(String, String)]]]()

  val runtimeStageTaskTracker = new RuntimeStageTaskTracker()
  val observedLogicalQS: mutable.Set[LogicalPlan] = mutable.Set[LogicalPlan]()
  val observedPhysicalQS: mutable.Set[SparkPlan] = mutable.Set[SparkPlan]()
  val qsTotalTaskDurationTracker = new QSTotalTaskDurationTracker()

  def getLqpId: Int = lqpId.get()
  def getQsId: Int = qsId.get()

  def addLQP(
      lqpUnit: LQPUnit,
      startTimeInMs: Long,
      snapshot: RunningQueryStageSnapshot,
      runtimeKnobsDict: Map[String, Array[(String, String)]]
  ): Int = {
    val curId = lqpId.getAndIncrement()
    lqpMap += (curId -> lqpUnit)
    lqpStartTimeInMsMap += (curId -> startTimeInMs)
    lqpSnapshot += (curId -> snapshot)
    lqpThetaR += (curId -> runtimeKnobsDict)
    curId
  }

  def addQS(
      qsUnit: QSUnit,
      startTimeInMs: Long,
      snapshot: RunningQueryStageSnapshot,
      runtimeKnobsDict: Map[String, Array[(String, String)]]
  ): Int = {
    val curId = qsId.getAndIncrement()
    qsMap += (curId -> qsUnit)
    qsStartTimeMap += (curId -> startTimeInMs)
    qsSnapshot += (curId -> snapshot)
    qsThetaR += (curId -> runtimeKnobsDict)
    curId
  }

  def setSQLStartTimeInMs(timeInMs: Long): Unit = {
    sqlStartTimeInMs = timeInMs
  }
  def setSQLEndTimeInMs(timeInMs: Long): Unit = {
    sqlEndTimeInMs = timeInMs
  }

  def lqpMapJsonStr: String = {
    assert(
      sqlStartTimeInMs > 0 && sqlEndTimeInMs > 0,
      "Assertion failed: cannot be exposed before sqlStartTimeInMs & sqlEndTimeInMs are defined."
    )
    val lqpMap2 = lqpMap.map(x =>
      (
        x._1.toString,
        x._2.json ~
          ("RunningQueryStageSnapshot" -> lqpSnapshot(x._1).toJson) ~
          ("StartTimeInMs" -> lqpStartTimeInMsMap(x._1)) ~
          ("DurationInMs" -> (sqlEndTimeInMs - lqpStartTimeInMsMap(x._1))) ~
          ("RuntimeConfiguration" -> lqpThetaR(x._1).map(y =>
            (y._1, y._2.toSeq)
          ))
      )
    )

    assert(
      qsMap.size == qsTotalTaskDurationTracker.rootRddId2StageIds.size,
      s"Assertion failed: ${qsMap.size} != ${qsTotalTaskDurationTracker.rootRddId2StageIds.size}"
    )

    val qsId2QSResultTimes =
      qsTotalTaskDurationTracker.getQsId2QSResultTimes(qsMap)
    val qsMap2 = qsMap.map(x =>
      (
        x._1.toString,
        x._2.json ~
          ("RunningQueryStageSnapshot" -> qsSnapshot(x._1).toJson) ~
          ("StartTimeInMs" -> qsStartTimeMap(x._1)) ~
          ("DurationInMs" -> qsId2QSResultTimes(x._1).DurationInMs) ~
          ("TotalTaskDurationInMs" ->
            qsId2QSResultTimes(x._1).totalTasksDurationInMs) ~
          ("RelevantStageIds" ->
            qsId2QSResultTimes(x._1).relevantStageIds.toList) ~
          ("RuntimeConfiguration" -> qsThetaR(x._1).map(y =>
            (y._1, y._2.toSeq)
          ))
      )
    )

    val json = ("RuntimeLQPs" -> lqpMap2.toMap) ~
      ("RuntimeQSs" -> qsMap2.toMap) ~
      ("SQLStartTimeInMs" -> sqlStartTimeInMs) ~
      ("SQLEndTimeInMs" -> sqlEndTimeInMs) ~
      ("SQLDurationInMs" -> (sqlEndTimeInMs - sqlStartTimeInMs))
    pretty(render(json))
  }

}
