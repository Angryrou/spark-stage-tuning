package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{F, LQPUnit, MyUnit}
import org.apache.spark.sql.SparkSession
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

import scala.collection.mutable

class InitialCollector extends MyUnit {

  val lqpMap: mutable.Map[String, LQPUnit] = mutable.TreeMap[String, LQPUnit]()

  val lqpDurationInMsMap: mutable.Map[String, Long] =
    mutable.TreeMap[String, Long]()

  var knobsDict: Option[Map[String, Array[(String, String)]]] = None

  def markConfiguration(spark: SparkSession): Unit = {
    knobsDict = Some(F.getAllConfiguration(spark))
  }

  override def toJson: JValue = {
    val json = lqpMap("collect").json ~
      ("DurationInMs" -> lqpDurationInMsMap("collect")) ~
      ("Configuration" -> knobsDict.get.map(x => (x._1, x._2.toSeq)))
    render(json)
  }
}
