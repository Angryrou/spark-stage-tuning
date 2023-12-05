package edu.polytechnique.cedar.spark.listeners
import edu.polytechnique.cedar.spark.collector.UdaoCollector
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerEvent,
  SparkListenerJobStart,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd,
  SparkListenerTaskStart
}
import org.apache.spark.sql.execution.ui.{
  SparkListenerOnQueryStageSubmitted,
  SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart
}

case class UDAOSparkListener(rc: UdaoCollector) extends SparkListener {

  override def onJobStart(
      jobStart: SparkListenerJobStart
  ): Unit = rc.onJobStart(jobStart)

  override def onStageSubmitted(
      stageSubmitted: SparkListenerStageSubmitted
  ): Unit = rc.onStageSubmitted(stageSubmitted)

  override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted
  ): Unit = rc.onStageCompleted(stageCompleted)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit =
    rc.onTaskStart(taskStart)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit =
    rc.onTaskEnd(taskEnd)

  override def onOtherEvent(event: SparkListenerEvent): Unit =
    event match {
      case e: SparkListenerOnQueryStageSubmitted => rc.onQueryStageSubmitted(e)
      case e: SparkListenerSQLExecutionStart     => rc.onSQLExecutionStart(e)
      case e: SparkListenerSQLExecutionEnd       => rc.onSQLExecutionEnd(e)
      case _                                     =>
    }

}
