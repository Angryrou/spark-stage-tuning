package edu.polytechnique.cedar.spark.sql.extensions
import edu.polytechnique.cedar.spark.sql.component.collectors.RuntimeCollector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import edu.polytechnique.cedar.spark.sql.component.F

case class UpdateThetaR(
    spark: SparkSession,
    rc: RuntimeCollector,
    updateLqpId: Int,
    debug: Boolean
) extends Rule[LogicalPlan] {

  private val target = Map(
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "16MB",
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "10MB",
    "spark.sql.shuffle.partitions" -> "100"
  )

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val executionId = F.getExecutionId(spark)
    // just for our experiment -- we do not have duplicated commands.
    assert(
      executionId.isDefined && executionId.get <= 1,
      "Assertion failed: we should not have executionId.isEmpty or executionId > 2"
    )
    if (executionId.get == 1) {
      val lqpId = rc.getLqpId
      if (lqpId == updateLqpId) { // happening before counting the LQP
        for ((k, v) <- target) {
          spark.conf.set(k, v)
        }
      }
      if (debug) {
        println(s"update runtime LQP-${lqpId} for execId=${executionId.get}")
      }
    }
    plan
  }
}
