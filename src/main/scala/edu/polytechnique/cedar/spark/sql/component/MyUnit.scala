package edu.polytechnique.cedar.spark.sql.component

import org.json4s
import org.json4s.jackson.JsonMethods.{compact, pretty}

trait MyUnit {
  def toJson: json4s.JValue
  override def toString: String = pretty(toJson)
  def toCompactString: String = compact(toJson)
}
