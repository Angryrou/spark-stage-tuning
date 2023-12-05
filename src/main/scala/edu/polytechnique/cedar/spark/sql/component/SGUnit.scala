package edu.polytechnique.cedar.spark.sql.component

// stage group unit
case class SGUnit(
    id: Int,
    sign: String,
    var stageIds: Seq[Int],
    table: String
)
