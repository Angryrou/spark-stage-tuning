package edu.polytechnique.cedar.spark.sql

import edu.polytechnique.cedar.spark.listeners.MySparkListener
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty

case class RunTemplateQueryConfig(
    benchmarkName: String = null, // TPCH / TPCDS
    scaleFactor: String = null, // 1
    queryLocationHeader: String = null,
    databaseName: String = null,
    queryName: String = null,
    templateName: String = null,
    localDebug: Boolean = false
)

object RunTemplateQuery {

  def main(args: Array[String]): Unit = {
    val parser =
      new scopt.OptionParser[RunTemplateQueryConfig]("Run-Benchmark-Query") {
        opt[String]('b', "benchmark")
          .action { (x, c) => c.copy(benchmarkName = x) }
          .text("the name of the benchmark to run")
          .required()
        opt[String]('t', "templateName")
          .action { (x, c) => c.copy(templateName = x) }
          .text("the templateName to run")
          .required()
        opt[String]('q', "queryName")
          .action { (x, c) => c.copy(queryName = x) }
          .text("the queryName to run")
          .required()
        opt[String]('s', "scaleFactor")
          .action((x, c) => c.copy(scaleFactor = x))
          .text(
            "scaleFactor defines the size of the dataset to generate (in GB)"
          )
          .required()
        opt[String]('l', "queryLocationHeader")
          .action((x, c) => c.copy(queryLocationHeader = x))
          .text("head root directory of all queries")
        opt[String]('n', "databaseName")
          .action((x, c) => c.copy(databaseName = x))
          .text("customized databaseName")
        opt[String]('d', "localDebug")
          .action((x, c) => c.copy(localDebug = x.toBoolean))
          .text("Local debug")
        help("help")
          .text("prints this usage text")
      }

    parser.parse(args, RunTemplateQueryConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunTemplateQueryConfig): Unit = {
    assert(config.benchmarkName == "TPCH" || config.benchmarkName == "TPCDS")
    val aggMetrics = AggMetrics()
    val spark = if (config.localDebug) {
      SparkSession
        .builder()
        .config("spark.master", "local[*]")
        .config("spark.default.parallelism", "40")
        .config("spark.reducer.maxSizeInFlight", "48m")
        .config("spark.shuffle.sort.bypassMergeThreshold", "200")
        .config("spark.shuffle.compress", "true")
        .config("spark.memory.fraction", "0.6")
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enable", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config(
          "spark.serializer",
          "org.apache.spark.serializer.KryoSerializer"
        )
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.yarn.historyServer.address", "http://localhost:18088")
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
    } else {
      SparkSession
        .builder()
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
    val databaseName =
      if (config.databaseName == null)
        s"${config.benchmarkName.toLowerCase}_${config.scaleFactor}"
      else config.databaseName
    val tid: String = config.templateName
    val qid: String = config.queryName
    val queryLocationHeader: String = config.queryLocationHeader

    spark.sql(s"use $databaseName")
    val source = scala.io.Source.fromFile(
      s"${queryLocationHeader}/${tid}/${tid}-${qid}.sql"
    )
    val queryContent: String =
      try source.mkString
      finally source.close()

    println(spark.sparkContext.applicationId)
    println(spark.sparkContext.getConf.get("spark.yarn.historyServer.address"))

    println(s"run ${queryLocationHeader}/${tid}/${tid}-${qid}.sql")
    println(queryContent)
    spark.sql(queryContent).collect()

    aggMetrics.runtimePlans.terminate()
    spark.close()

    println("---- Initial Plan ----")
    println(aggMetrics.initialPlans.toString)
    println("---- Runtime Plan ----")
    println(aggMetrics.runtimePlans.toString)
    println("---- Query Time Metric ----")
    println(s"${writePretty(aggMetrics.initialPlanTimeMetric)(DefaultFormats)}")
    println("---- Run Time Metrics - stageSubmittedTime")
    println(s"${writePretty(aggMetrics.stageSubmittedTime)(DefaultFormats)}")
    println("---- Run Time Metrics - stageCompletedTime")
    println(s"${writePretty(aggMetrics.stageCompletedTime)(DefaultFormats)}")
    println("---- Run Time Metrics - stageFirstTaskTime")
    println(s"${writePretty(aggMetrics.stageFirstTaskTime)(DefaultFormats)}")
    println("---- Run Time Metrics - stageTotalTaskTime")
    println(s"${writePretty(aggMetrics.stageTotalTaskTime)(DefaultFormats)}")

  }
}
