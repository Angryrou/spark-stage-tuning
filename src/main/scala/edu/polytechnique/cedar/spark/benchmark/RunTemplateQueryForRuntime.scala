package edu.polytechnique.cedar.spark.benchmark

import edu.polytechnique.cedar.spark.benchmark.config.RunTemplateQueryConfig
import edu.polytechnique.cedar.spark.listeners.{
  UDAOQueryExecutionListener,
  UDAOSparkListener
}
import edu.polytechnique.cedar.spark.sql.component.collectors.{
  InitialCollector,
  RuntimeCollector
}
import edu.polytechnique.cedar.spark.sql.extensions.ExposeRuntimeLogicalPlan
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter

object RunTemplateQueryForRuntime {

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
    val initialCollector = new InitialCollector()
    val runtimeCollector = new RuntimeCollector()
    val spark = if (config.localDebug) {
      SparkSession
        .builder()
        .appName(
          s"${config.benchmarkName}_${config.templateName}-${config.queryName}"
        )
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
          extensions.injectRuntimeOptimizerPrefixRule(
            ExposeRuntimeLogicalPlan(_, runtimeCollector, config.localDebug)
          )
        }
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .withExtensions { extensions =>
          extensions.injectRuntimeOptimizerPrefixRule(
            ExposeRuntimeLogicalPlan(_, runtimeCollector, config.localDebug)
          )
        }
        .enableHiveSupport()
        .getOrCreate()
    }

    spark.listenerManager.register(
      UDAOQueryExecutionListener(initialCollector, config.localDebug)
    )
    spark.sparkContext.addSparkListener(
      UDAOSparkListener(runtimeCollector, config.localDebug)
    )
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
    spark.close()

    val writer1 = new PrintWriter(
      s"./outs/initial/${spark.sparkContext.appName}_${spark.sparkContext.applicationId}.json"
    )
    val jsonString = initialCollector.toString
    // println(jsonString)
    writer1.write(jsonString)
    writer1.close()

    val writer2 = new PrintWriter(
      s"./outs/runtime/${spark.sparkContext.appName}_${spark.sparkContext.applicationId}_lqps.json"
    )
    val lqpJsonString = runtimeCollector.lqpMapJsonStr
    // println(lqpJsonString)
    writer2.write(lqpJsonString)
    writer2.close()
  }
}
