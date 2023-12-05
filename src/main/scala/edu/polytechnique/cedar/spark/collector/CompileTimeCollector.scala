package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.F.KnobKV
import edu.polytechnique.cedar.spark.sql.component.{
  F,
  IOBytesUnit,
  LQPUnit,
  MyUnit
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

import scala.collection.concurrent.TrieMap

class CompileTimeCollector {

  private var lqpUnit: Option[LQPUnit] = None
  private var thetaMap: Option[Map[String, Array[KnobKV]]] = None

  def onCompile(spark: SparkSession, queryContent: String): Unit = {
    thetaMap = Some(F.getAllConfiguration(spark))
    val plan = spark.sql(queryContent).queryExecution.optimizedPlan
    lqpUnit = Some(F.exportLQP(plan))
  }

  def exposeJson: JsonAST.JObject = {
    lqpUnit.get.json ~
      ("Configuration" -> thetaMap.get.map(x => (x._1, x._2.toList)))
  }
}
