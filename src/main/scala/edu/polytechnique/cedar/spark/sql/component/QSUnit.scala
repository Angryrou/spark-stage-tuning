package edu.polytechnique.cedar.spark.sql.component
import org.apache.spark.sql.execution.SparkPlan
import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class QSUnit(
    qsId: Int,
    tag: SparkPlan,
    var qsParentIds: Seq[Int],
    var parentTags: Seq[SparkPlan],
    qsMetrics: QSUnitMetrics,
    var hopMap: Seq[(String, Int)],
    var isFinalHopMap: Boolean,
    optimizationTimeInMs: Long,
    stageSnapshot: RunningQueryStageSnapshot,
    thetaR: Map[String, Array[(String, String)]]
) extends StageBaseUnit(qsId, qsParentIds)
    with MyUnit {

  // should not be used. only for testing
  def updateWithSubqueries(
      subqueryIds: Seq[Int],
      subqueryTags: Seq[SparkPlan]
  ): Unit = {
    parentIds ++= subqueryIds
    parentTags ++= subqueryTags
  }

  val json: JsonAST.JObject = qsMetrics.json ~
    ("OptimizationTimeInMs" -> optimizationTimeInMs) ~
    ("RunningQueryStageSnapshot" -> stageSnapshot.toJson) ~
    ("RuntimeConfiguration" -> thetaR.map(x => (x._1, x._2.toList))) ~
    ("ParentQueryStageIds" -> qsParentIds.toList)

  override def toJson: JValue = render(json)

  def getHopMapSign: String = F.serializeHopMap(hopMap)
}
