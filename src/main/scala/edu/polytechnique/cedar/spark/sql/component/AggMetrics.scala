package edu.polytechnique.cedar.spark.sql.component

import edu.polytechnique.cedar.spark.sql.{
  InitialPlanTimeMetric,
  InitialPlans,
  RuntimePlans
}

import scala.collection.mutable

case class AggMetrics() {

  val initialPlanTimeMetric: InitialPlanTimeMetric = InitialPlanTimeMetric(
    queryStartTimeMap =
      mutable.TreeMap[Long, Long](), // executionId to queryStartTime
    queryEndTimeMap =
      mutable.TreeMap[Long, Long]() // executionId to queryEndTime
  )
  val runtimePlans: RuntimePlans = new RuntimePlans()

  val stageSubmittedTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()
  val stageCompletedTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()
  val stageFirstTaskTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()
  val stageTotalTaskTime: mutable.TreeMap[Int, Long] =
    mutable.TreeMap[Int, Long]()

  var successFlag: Boolean = true
}
