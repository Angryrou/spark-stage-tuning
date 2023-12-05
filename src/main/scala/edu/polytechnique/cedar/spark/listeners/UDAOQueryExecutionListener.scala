package edu.polytechnique.cedar.spark.listeners
import edu.polytechnique.cedar.spark.collector.CompileTimeCollector
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

case class UDAOQueryExecutionListener(initialCollector: CompileTimeCollector)
    extends QueryExecutionListener {

  override def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long
  ): Unit = initialCollector.onSuccess(funcName, qe, durationNs)

  override def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception
  ): Unit = {}
}
