package edu.polytechnique.cedar.spark.collector.runtime
import edu.polytechnique.cedar.spark.sql.component.F.KnobKV

import java.util.concurrent.atomic.AtomicInteger
import edu.polytechnique.cedar.spark.sql.component.{LQPUnit, RunningSnapshot}
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import scala.collection.concurrent.TrieMap
class RuntimeLogicalQueryPlanCollector {

  private val lqpId = new AtomicInteger(1)
  private val lqpUnitMap = new TrieMap[Int, LQPUnit]()
  private val startTimeInMsMap = new TrieMap[Int, Long]()
  private val snapshotMap = new TrieMap[Int, RunningSnapshot]()
  private val thetaRMap = new TrieMap[Int, Map[String, Array[KnobKV]]]()

  def getLqpId: Int = lqpId.get()

  def addLQP(
      lqpUnit: LQPUnit,
      startTimeInMs: Long,
      snapshot: RunningSnapshot,
      runtimeKnobsDict: Map[String, Array[KnobKV]]
  ): Int = {
    val curId = lqpId.getAndIncrement()
    lqpUnitMap += (curId -> lqpUnit)
    startTimeInMsMap += (curId -> startTimeInMs)
    snapshotMap += (curId -> snapshot)
    thetaRMap += (curId -> runtimeKnobsDict)
    curId
  }

  def exposeMap(sqlEndTimeInMs: Long): Map[String, JsonAST.JObject] =
    lqpUnitMap
      .map(x =>
        (
          x._1.toString,
          x._2.json ~
            ("RunningQueryStageSnapshot" -> snapshotMap(x._1).toJson) ~
            ("StartTimeInMs" -> startTimeInMsMap(x._1)) ~
            ("DurationInMs" -> (sqlEndTimeInMs - startTimeInMsMap(x._1))) ~
            ("RuntimeConfiguration" -> thetaRMap(x._1)
              .map(y => (y._1, y._2.toSeq)))
        )
      )
      .toMap
}
