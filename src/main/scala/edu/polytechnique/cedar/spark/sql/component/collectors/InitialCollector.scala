package edu.polytechnique.cedar.spark.sql.component.collectors

import edu.polytechnique.cedar.spark.sql.component.{LQPUnit, MyUnit}
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

import scala.collection.mutable

class InitialCollector extends MyUnit {

  val lqpMap: mutable.Map[String, LQPUnit] = mutable.TreeMap[String, LQPUnit]()

  val lqpLatsMap: mutable.Map[String, Long] = mutable.TreeMap[String, Long]()

  override def toJson: JValue = {
    val json = lqpMap("collect").json ~ ("LatNs" -> lqpLatsMap("collect"))
    render(json)
  }
}
