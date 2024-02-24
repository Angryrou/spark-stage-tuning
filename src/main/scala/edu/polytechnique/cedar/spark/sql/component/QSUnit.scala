package edu.polytechnique.cedar.spark.sql.component

import edu.polytechnique.cedar.spark.sql.component.F.KnobKV
import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render
case class QSUnit(
    id: Int,
    qsOptId: Int,
    qsUnitMetrics: QSMetrics,
    durationInMs: Long,
    totalTasksDurationInMs: Long,
    ioBytes: IOBytesUnit,
    snapshot: RunningSnapshot,
    runtimeOptSolvingMeasure: RuntimeOptMeasureUnit,
    thetaR: Map[String, Array[KnobKV]],
    relevantStages: Seq[Int],
    table: String
) extends MyUnit {

  val json: JsonAST.JObject = qsUnitMetrics.json ~
    ("RunningQueryStageSnapshot" -> snapshot.toJson) ~
    ("QueryStageOptimizationId" -> qsOptId) ~
    ("RuntimeConfiguration" -> thetaR.map(x => (x._1, x._2.toList))) ~
    ("RuntimeOptSolvingMeasure" -> runtimeOptSolvingMeasure.toJson) ~
    ("RelevantQueryStageIds" -> relevantStages.toList) ~
    ("Objectives" -> ("DurationInMs" -> durationInMs) ~
      ("TotalTasksDurationInMs" -> totalTasksDurationInMs) ~
      ("IOBytes" -> ioBytes.json))
  override def toJson: JValue = render(json)
}
