package edu.polytechnique.cedar.spark.benchmark

import edu.polytechnique.cedar.spark.benchmark.config.RunTemplateQueryConfig
import org.apache.spark.sql.SparkSession


object RunTemplateQueryRaw {

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
        opt[String]('x', "extractedPath")
          .action((x, c) => c.copy(extractedPath = x))
          .text("customized path for the extracted traces")
          .required()
        opt[String]('d', "localDebug")
          .action((x, c) => c.copy(localDebug = x.toBoolean))
          .text("Local debug")
        opt[String]('v', "verbose")
          .action((x, c) => c.copy(verbose = x.toBoolean))
          .text("Verbose")
        opt[String]('u', "enableRuntimeSolver")
          .action((x, c) => c.copy(enableRuntimeSolver = x.toBoolean))
        opt[String]("runtimeSolverHost")
          .action((x, c) => c.copy(runtimeSolverHost = x))
        opt[String]("runtimeSolverPort")
          .action((x, c) => c.copy(runtimeSolverPort = x.toInt))
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
    assert(config.benchmarkName == "tpch" || config.benchmarkName == "tpcds")
    val tid: String = config.templateName
    val qid: String = config.queryName
    val debug = config.localDebug
    val spark = if (debug) {
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
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enable", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config(
          "spark.serializer",
          "org.apache.spark.serializer.KryoSerializer"
        )
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.yarn.historyServer.address", "http://localhost:18088")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .enableHiveSupport()
        .getOrCreate()
    }

    val databaseName =
      if (config.databaseName == null)
        s"${config.benchmarkName}_${config.scaleFactor}"
      else config.databaseName
    val queryLocationHeader: String = config.queryLocationHeader

    spark.sql(s"use $databaseName")
    val source = scala.io.Source.fromFile(
      s"${queryLocationHeader}/${tid}/${tid}-${qid}.sql"
    )
    val queryContent: String =
      try source.mkString
      finally source.close()

    // Start timing
    val startTime = System.nanoTime()
    // Execute the query
    spark.sql(queryContent)
    // Stop timing
    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9d // Convert to seconds
    println(s"$tid: ${durationSeconds}s")
  }
}
