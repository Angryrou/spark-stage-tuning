package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{QSResultTimes, QSUnit}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class QSTotalTaskDurationTracker {
  val stageTotalTasksDurationDict: TrieMap[Int, Long] = new TrieMap[Int, Long]()
  val stageStartEndTimeDict: TrieMap[Int, (Long, Long)] =
    new TrieMap[Int, (Long, Long)]()

  val table2QSIds: TrieMap[String, Array[Int]] =
    new TrieMap[String, Array[Int]]()

  val table2minRddIds: TrieMap[String, Array[(Int, String)]] =
    new TrieMap[String, Array[(Int, String)]]()

  val minRddId2StageIds: TrieMap[(Int, String), Array[Int]] =
    new TrieMap[(Int, String), Array[Int]]()
  val listLeafStageIds: mutable.Set[Int] = mutable.Set[Int]()

  private def getMinRddId2QSResultTimes
      : mutable.Map[(Int, String), QSResultTimes] = {
    val minRddId2QSResultTimes = mutable.TreeMap[(Int, String), QSResultTimes]()

    for (((rootRddIds, wscgSign), stageIds) <- minRddId2StageIds) {
      val startTime = stageIds.map(stageStartEndTimeDict(_)._1).min
      val endTime = stageIds.map(stageStartEndTimeDict(_)._2).max
      val durationInMs = endTime - startTime
      val totalTasksDurationInMs =
        stageIds.map(stageTotalTasksDurationDict(_)).sum
      val qSResultTimes =
        QSResultTimes(durationInMs, totalTasksDurationInMs, stageIds)
      minRddId2QSResultTimes += ((rootRddIds, wscgSign) -> qSResultTimes)
    }
    minRddId2QSResultTimes
  }

  def getQsId2QSResultTimes(
      qsMap: TrieMap[Int, QSUnit],
      verbose: Boolean = false
  ): Map[Int, QSResultTimes] = {
    val minRddId2QSResultTimes = getMinRddId2QSResultTimes
    assert(qsMap.size == minRddId2QSResultTimes.size)
    assert(table2QSIds.keySet == table2minRddIds.keySet)
    val keySet = table2QSIds.keySet
    val qsId2QSResultTimes = TrieMap[Int, QSResultTimes]()
    val qs2stageIds = mutable.TreeMap[Int, String]()
    for (key <- keySet) {
      val qsIds = table2QSIds(key).sorted
      val minRddIds = table2minRddIds(key).sorted
      assert(qsIds.length == minRddIds.length)
      qsIds
        .zip(minRddIds)
        .foreach(x => {
          assert(!qsId2QSResultTimes.contains(x._1))
          qs2stageIds.update(
            x._1,
            minRddId2QSResultTimes(x._2).relevantStageIds.mkString(",")
          )
          qsId2QSResultTimes.update(x._1, minRddId2QSResultTimes(x._2))
        })
    }
    if (verbose) {
      qs2stageIds.foreach(x => println(s"QS${x._1} -> stages [${x._2}]"))
    }
    qsId2QSResultTimes.toMap
  }
}
