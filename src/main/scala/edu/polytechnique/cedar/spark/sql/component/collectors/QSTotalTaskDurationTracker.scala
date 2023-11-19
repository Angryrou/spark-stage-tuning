package edu.polytechnique.cedar.spark.sql.component.collectors

import scala.collection.mutable

class QSTotalTaskDurationTracker {
  val stageTotalTasksDurationDict: mutable.Map[Int, Long] =
    mutable.Map[Int, Long]()
  val rootRddId2StageIds: mutable.Map[Int, Array[Int]] =
    mutable.Map[Int, Array[Int]]()
  val listLeafStageIds: mutable.Set[Int] = mutable.Set[Int]()
}
