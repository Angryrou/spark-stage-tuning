package edu.polytechnique.cedar.spark.sql.component

// stage group results
case class SGResults(
    id: Int,
    durationInMs: Long,
    totalTasksDurationInMs: Long,
    relevantStages: Seq[Int],
    ioBytes: IOBytesUnit
)
