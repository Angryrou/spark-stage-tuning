package edu.polytechnique.cedar.spark.sql.component

// Unit of a Spark stage

case class SUnit(
    id: Int,
    rddIds: Seq[Int],
    rddScopeIdsSign: String,
    parentStageIds: Seq[Int],
    hopMap: Seq[(String, Int)]
) {
  override def toString: String =
    s"Stage(id=${id}, " +
      s"rddIds=${rddIds.mkString(",")}, " +
      s"rddScopeIds=$rddScopeIdsSign, " +
      s"parentStageIds=${parentStageIds.mkString(",")}, " +
      s"hopMap=${F.serializeHopMap(hopMap)}"
}
