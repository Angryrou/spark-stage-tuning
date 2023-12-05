package edu.polytechnique.cedar.spark.sql.component

case class QSIndex(
    waveId: Int,
    idInWave: Int,
    planId: Int,
    qsId: Int,
    optimizedStageOrder: Int,
    subqueryIds: Seq[Int],
    isSubquery: Boolean,
    isLastQueryStage: Boolean
) {

  def hasSubqueries: Boolean = subqueryIds.nonEmpty
  override def toString: String = {
    s"QSIndex(waveId=$waveId, " +
      s"idInWave=$idInWave, " +
      s"planId=$planId, " +
      s"qsId=$qsId, " +
      s"optimizedStageOrder=$optimizedStageOrder, " +
      s"subqueryIds=${subqueryIds.mkString(",")}, " +
      s"isLastQueryStage=$isLastQueryStage)"
  }
}
