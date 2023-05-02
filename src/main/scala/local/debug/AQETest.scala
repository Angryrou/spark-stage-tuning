package local.debug

import edu.polytechnique.cedar.spark.listeners.MySparkListener
import edu.polytechnique.cedar.spark.sql.{
  AggMetrics,
  ExportInitialPlan,
  ExportRuntimeQueryStage
}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty

object AQETest {

  def main(args: Array[String]): Unit = {
//    val initialPlans: mutable.Map[Long, InitialPlan] = mutable.Map[Long, InitialPlan]()
//    val initialPlanTimeMetric: InitialPlanTimeMetric = InitialPlanTimeMetric(
//      queryStartTimeMap = mutable.Map[Long, Long](), // executionId to queryStartTime
//      queryEndTimeMap = mutable.Map[Long, Long]()) // executionId to queryEndTime
    val aggMetrics = AggMetrics()

    val spark =
      try {
        SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()
      } catch {
        case _ =>
          SparkSession
            .builder()
            .config("spark.master", "local[2]")
            .config("spark.default.parallelism", "4")
            .config("spark.sql.adaptive.enable", "true")
            .withExtensions { extensions =>
              extensions.injectQueryStagePrepRule(
                ExportInitialPlan(_, aggMetrics.initialPlans)
              )
              extensions.injectQueryStageOptimizerRule(
                ExportRuntimeQueryStage(_, aggMetrics.runtimePlans)
              )
            }
            .enableHiveSupport()
            .getOrCreate()
      }

    spark.sparkContext.addSparkListener(MySparkListener(aggMetrics))
//    spark.listenerManager.register(MyQueryExecutionListener())

    //    spark.sql("show databases").show()
    //    spark.sql("create database if not exists debug").show()
    spark.sql("use debug")

    //    val queryContent1: String =
    //    """
    //      |CREATE TABLE if not exists dealer (id INT, city STRING, car_model STRING, quantity INT);
    //      |""".stripMargin
    //
    //    println(queryContent1)
    //    spark.sql(queryContent1).collect()
    //
    //    val queryContent2: String =
    //      """
    //        |INSERT INTO dealer VALUES
    //        |    (100, 'Fremont', 'Honda Civic', 10),
    //        |    (100, 'Fremont', 'Honda Accord', 15),
    //        |    (100, 'Fremont', 'Honda CRV', 7),
    //        |    (200, 'Dublin', 'Honda Civic', 20),
    //        |    (200, 'Dublin', 'Honda Accord', 10),
    //        |    (200, 'Dublin', 'Honda CRV', 3),
    //        |    (300, 'San Jose', 'Honda Civic', 5),
    //        |    (300, 'San Jose', 'Honda Accord', 8);
    //        |""".stripMargin
    //    println(queryContent2)
    //    spark.sql(queryContent2).collect()

    val queryContent3: String =
      """
        |SELECT d1.id, d2.id, AVG(d1.quantity + d2.quantity) as qq
        |FROM dealer d1, dealer d2
        |WHERE d1.id < d2.id
        |GROUP BY d1.id, d2.id ORDER BY d1.id;
        |""".stripMargin

    println(queryContent3)

    println(s"Before Execution: ${aggMetrics.initialPlans.toString()}")
    spark.sql(queryContent3).collect()
    //    val df = spark.sql(queryContent3)
    //    spark.conf.set("spark.default.parallelism", 16)
    //    println(spark.conf.get("spark.default.parallelism"))
    //    println(df.explain("cost"))
    //    df.cache()
    //    df.collect()
    //    df.show()
//    println(s"After Execution: ${writePretty(aggMetrics.initialPlans)(DefaultFormats)}")
//    println(s"After Execution: ${writePretty(aggMetrics.runtimePlans)(DefaultFormats)}")
    println(
      s"Query Time Metric: ${writePretty(aggMetrics.initialPlanTimeMetric)(DefaultFormats)}"
    )

    println("done.")
  }
}
