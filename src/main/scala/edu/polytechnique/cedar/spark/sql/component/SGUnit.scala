package edu.polytechnique.cedar.spark.sql.component

// Unit of a Spark stage group for the same QS, with a distinct key of `rddScopeIdsSign`

case class SGUnit(
    sgId: Int,
    sgSign: String,
    stageIds: Seq[Int],
    parentSGIds: Seq[Int],
    hopMap: Seq[(String, Int)]
) extends StageBaseUnit(sgId, parentSGIds) {
  override def toString: String =
    s"StageGroup(id=${id}, " +
      s"sgSign=$sgSign, " +
      s"stageIds=${stageIds.mkString(",")}, " +
      s"parentSGIds=${parentIds.mkString(",")}, " +
      s"hopMap=$getHopMapSign"

  def getHopMapSign: String = F.serializeHopMap(hopMap)

}
