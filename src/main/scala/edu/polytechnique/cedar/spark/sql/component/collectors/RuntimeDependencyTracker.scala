package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{
  SGUnit,
  SUnit,
  QSResultTimes
}
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.storage.RDDInfo

import scala.collection.concurrent.TrieMap

class RuntimeDependencyTracker {
  val rddId2StageIds: TrieMap[Int, Set[Int]] = new TrieMap[Int, Set[Int]]()
  val stageMap: TrieMap[Int, SUnit] = new TrieMap[Int, SUnit]()

  val sgId2sgSign = new TrieMap[Int, String]()
  val sgSign2sgId = new TrieMap[String, Int]()
  val stageGroupMap: TrieMap[Int, SGUnit] = new TrieMap()

  val stageTotalTasksDurationDict: TrieMap[Int, Long] = new TrieMap()
  val stageStartEndTimeDict: TrieMap[Int, (Long, Long)] = new TrieMap()

  private val tableCounter: TrieMap[String, Int] = new TrieMap()

  def addStage(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val rddInfos = stageSubmitted.stageInfo.rddInfos
    val stageId = stageSubmitted.stageInfo.stageId

    // stage info
    val rddIds: Seq[Int] = rddInfos.map(_.id).sorted
    val rddScopeIds = rddInfos
      .map(_.scope.get.id.toInt)
      .distinct
      .sorted
    val rddScopeIdsSign = rddScopeIds.mkString(",")
    val parentRddIds: Seq[Int] = rddInfos.flatMap(_.parentIds).sorted
    val parentStageIds = (parentRddIds.toSet -- rddIds.toSet)
      .map { id =>
        assert(rddId2StageIds.contains(id) && rddId2StageIds(id).size == 1)
        rddId2StageIds(id).head
      }
      .toSeq
      .sorted
    val nonParentRddInfos: Seq[RDDInfo] =
      rddInfos.filterNot(rddInfo => rddInfo.parentIds.nonEmpty)
    nonParentRddInfos.foreach(rddInfo => assert(rddInfo.name == "FileScanRDD"))
    val hopMap = (parentStageIds.flatMap(
      stageMap(_).hopMap.map(x => (x._1, x._2 + 1))
    ) ++ nonParentRddInfos.map { rddInfo =>
      val table = rddInfo.scope.get.name.split('.').last
      tableCounter.get(table) match {
        case Some(count) =>
          tableCounter.update(table, count + 1)
          (table + (count + 1).toString, 0)
        case None =>
          tableCounter.update(table, 1)
          (table + "1", 0)
      }
    }).sorted

    // Add a SUnit to stageMap
    stageMap += (stageId -> SUnit(
      stageId,
      rddIds,
      rddScopeIdsSign,
      parentStageIds,
      hopMap
    ))

    // Add to stageGroupMap (add or update)
    sgSign2sgId.get(rddScopeIdsSign) match {
      case Some(sgId) =>
        assert(
          sgId2sgSign(sgId) == rddScopeIdsSign && stageGroupMap.contains(sgId)
        )
        val sgUnitOld = stageGroupMap(sgId)
        assert(
          sgUnitOld.id == sgId
            && sgUnitOld.sgSign == rddScopeIdsSign
            && sgUnitOld.parentSGIds == parentStageIds
              .map(id => sgSign2sgId(stageMap(id).rddScopeIdsSign))
            && sgUnitOld.hopMap == hopMap
        )
        stageGroupMap.update(
          sgId,
          SGUnit(
            sgUnitOld.id,
            sgUnitOld.sgSign,
            sgUnitOld.stageIds ++ Seq(stageId),
            sgUnitOld.parentSGIds,
            sgUnitOld.hopMap
          )
        )
      case None =>
        val sgId = sgId2sgSign.size
        assert(!stageGroupMap.contains(sgId))
        sgSign2sgId += (rddScopeIdsSign -> sgId)
        sgId2sgSign += (sgId -> rddScopeIdsSign)
        stageGroupMap += (sgId -> SGUnit(
          sgId,
          rddScopeIdsSign,
          Seq(stageId),
          parentStageIds.map(id => sgSign2sgId(stageMap(id).rddScopeIdsSign)),
          hopMap
        ))
    }

    // update rddId2StageIds
    rddIds.foreach(rddId =>
      rddId2StageIds.get(rddId) match {
        case Some(stageIds) =>
          rddId2StageIds.update(rddId, stageIds ++ Set(stageId))
        case None =>
          rddId2StageIds.update(rddId, Set(stageId))
      }
    )
  }

  def getQsId2QSResultTimes(
      qsId2sgIdMapping: Map[Int, Int]
  ): Map[Int, QSResultTimes] = {
    val qsId2QSResultTimes = TrieMap[Int, QSResultTimes]()
    for ((qsId, sgId) <- qsId2sgIdMapping) {
      val stageIds = stageGroupMap(sgId).stageIds.sorted
      val startTime = stageIds.map(stageStartEndTimeDict(_)._1).min
      val endTime = stageIds.map(stageStartEndTimeDict(_)._2).max
      val durationInMs = endTime - startTime
      val totalTasksDurationInMs =
        stageIds.map(stageTotalTasksDurationDict(_)).sum
      qsId2QSResultTimes.update(
        qsId,
        QSResultTimes(durationInMs, totalTasksDurationInMs, stageIds)
      )
    }
    qsId2QSResultTimes.toMap
  }

}
