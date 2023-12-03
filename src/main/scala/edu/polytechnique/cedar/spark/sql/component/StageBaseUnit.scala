package edu.polytechnique.cedar.spark.sql.component

abstract class StageBaseUnit(
    val id: Int,
    var parentIds: Seq[Int]
) {
  def getHopMapSign: String
}
