package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.F.KnobKV
import edu.polytechnique.cedar.spark.sql.component.{F, LQPUnit}
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST
import org.json4s.JsonDSL._

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
