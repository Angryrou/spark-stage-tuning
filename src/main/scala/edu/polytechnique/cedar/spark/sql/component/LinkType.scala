package edu.polytechnique.cedar.spark.sql.component

object LinkType extends Enumeration {
  type LinkType = Value
  val Operator, Subquery, Shuffle, Broadcast, SubqueryBroadcast = Value
}
