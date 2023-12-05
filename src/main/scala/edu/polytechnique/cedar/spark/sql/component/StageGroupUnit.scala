package edu.polytechnique.cedar.spark.sql.component

case class StageGroupUnit(
    id: Int,
    sign: String,
    var stageIds: Seq[Int],
    table: String
)
