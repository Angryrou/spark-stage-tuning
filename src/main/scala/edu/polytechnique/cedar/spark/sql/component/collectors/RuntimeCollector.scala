package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{
  LQPUnit,
  QSUnit,
  RunningQueryStageSnapshot
}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class RuntimeCollector() {

  private val lqpId: AtomicInteger = new AtomicInteger(1)
  val lqpMap: mutable.Map[Int, LQPUnit] = mutable.TreeMap[Int, LQPUnit]()
  private val lqpStartTimeInMsMap: mutable.Map[Int, Long] =
    mutable.TreeMap[Int, Long]()
  private val lqpSnapshot: mutable.Map[Int, RunningQueryStageSnapshot] =
    mutable.TreeMap[Int, RunningQueryStageSnapshot]()
  private var sqlStartTimeInMs: Long = -1
  private var sqlEndTimeInMs: Long = -1

  val qsId: AtomicInteger = new AtomicInteger(0)
  val qsMap: mutable.Map[Int, QSUnit] = mutable.TreeMap[Int, QSUnit]()
  val qsStartTimeMap: mutable.Map[Int, Long] = mutable.TreeMap[Int, Long]()

  val runtimeStageTaskTracker = new RuntimeStageTaskTracker()

  def addLQP(
      lqpUnit: LQPUnit,
      startTimeInMs: Long,
      snapshot: RunningQueryStageSnapshot
  ): Int = {
    val curId = lqpId.getAndIncrement()
    lqpMap += (curId -> lqpUnit)
    lqpStartTimeInMsMap += (curId -> startTimeInMs)
    lqpSnapshot += (curId -> snapshot)
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
          ("DurationInMs" -> (sqlEndTimeInMs - lqpStartTimeInMsMap(x._1)))
      )
    )
    val json = ("runtimeLQPs" -> lqpMap2.toMap) ~
      ("SQLStartTimeInMs" -> sqlStartTimeInMs) ~
      ("SQLEndTimeInMs" -> sqlEndTimeInMs) ~
      ("SQLDurationInMs" -> (sqlEndTimeInMs - sqlStartTimeInMs))
    pretty(render(json))
  }

}
