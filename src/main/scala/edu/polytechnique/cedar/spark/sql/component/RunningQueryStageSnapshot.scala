package edu.polytechnique.cedar.spark.sql.component

import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class RunningQueryStageSnapshot(
    runtimeStageRunningTasksNum: Int,
    runtimeStageFinishedTasksNum: Int,
    runtimeStageFinishedTasksTotalTimeInMs: Double,
    runtimeStageFinishedTasksDistributionInMs: Seq[Double]
) extends MyUnit {

  val json: JsonAST.JObject =
    ("RunningTasksNum" -> runtimeStageRunningTasksNum) ~
      ("FinishedTasksNum" -> runtimeStageFinishedTasksNum) ~
      ("FinishedTasksTotalTimeInMs" -> runtimeStageFinishedTasksTotalTimeInMs) ~
      ("FinishedTasksDistributionInMs" -> runtimeStageFinishedTasksDistributionInMs.toList)
  override def toJson: JValue = render(json)
}
