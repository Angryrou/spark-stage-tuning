package edu.polytechnique.cedar.spark.sql.component

case class QSResultTimes(
    DurationInMs: Long,
    totalTasksDurationInMs: Long,
    relevantStageIds: Array[Int]
)
