package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{LQPUnit, MyUnit}
import org.apache.spark.sql.SparkSession
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

import scala.collection.mutable

class InitialCollector extends MyUnit {

  val lqpMap: mutable.Map[String, LQPUnit] = mutable.TreeMap[String, LQPUnit]()

  val lqpDurationInMsMap: mutable.Map[String, Long] =
    mutable.TreeMap[String, Long]()

  private val knobNames = Seq(
    /* context parameters (theta_c) */
    "spark.executor.memory", // k1
    "spark.executor.cores", // k2
    "spark.executor.instances", // k3
    "spark.default.parallelism", // k4
    "spark.reducer.maxSizeInFlight", // k5
    "spark.shuffle.sort.bypassMergeThreshold", // k6
    "spark.shuffle.compress", // k7
    "spark.memory.fraction", // k8

    /* logical query plan (LQP) parameters (theta_p) */
    "spark.sql.adaptive.advisoryPartitionSizeInBytes", // s1
    "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", // s2
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", // s3
    "spark.sql.adaptive.autoBroadcastJoinThreshold", // s4
    "spark.sql.shuffle.partitions", // s5
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", // s6
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor", // s7

    /* query stage (QS) parameters (theta_s) */
    "spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor", // s8
    "spark.sql.adaptive.coalescePartitions.minPartitionSize" // s9
  )
  val knobList: mutable.ArrayBuffer[(String, String)] = mutable.ArrayBuffer()
  def markConfiguration(spark: SparkSession): Unit = {
    for (k <- knobNames) {
      knobList += ((k, spark.conf.getOption(k).getOrElse("not found")))
    }
  }

  override def toJson: JValue = {
    val json = lqpMap("collect").json ~
      ("DurationInMs" -> lqpDurationInMsMap("collect")) ~
      ("Configuration" -> knobList)
    render(json)
  }
}
