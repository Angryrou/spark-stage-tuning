package edu.polytechnique.cedar.spark.sql.component

case class GroupStageResults(
    id: Int,
    durationInMs: Long,
    totalTasksDurationInMs: Long,
    relevantStages: Seq[Int],
    ioBytes: IOBytesUnit
)
