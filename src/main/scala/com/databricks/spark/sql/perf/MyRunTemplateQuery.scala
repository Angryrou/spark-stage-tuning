package com.databricks.spark.sql.perf

import org.apache.spark.sql.SparkSession

case class RunTemplateQueryConfig
(
  benchmarkName: String = null, // TPCH / TPCDS
  scaleFactor: String = null, // 1
  queryLocationHeader: String = null,
  databaseName: String = null,
  queryName: String = null,
  templateName: String = null
)

object MyRunTemplateQuery {

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunTemplateQueryConfig]("Run-Benchmark-Query") {
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
        .text("scaleFactor defines the size of the dataset to generate (in GB)")
        .required()
      opt[String]('l', "queryLocationHeader")
        .action((x, c) => c.copy(queryLocationHeader = x))
        .text("head root directory of all queries")
      opt[String]('n', "databaseName")
        .action((x, c) => c.copy(databaseName = x))
        .text("customized databaseName")
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

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val databaseName = if (config.databaseName == null) s"${config.benchmarkName.toLowerCase}_${config.scaleFactor}" else config.databaseName
    val tid: String = config.templateName
    val qid: String = config.queryName
    val queryLocationHeader: String = config.queryLocationHeader

    spark.sql(s"use $databaseName")
    val source = scala.io.Source.fromFile(s"${queryLocationHeader}/${tid}/${tid}-${qid}.sql")
    val queryContent: String = try source.mkString finally source.close()

    println(spark.sparkContext.applicationId)
    println(spark.sparkContext.getConf.get("spark.yarn.historyServer.address"))

    println(s"run ${queryLocationHeader}/${tid}/${tid}-${qid}.sql")
    println(queryContent)
    spark.sql(queryContent).collect()

    spark.close()
  }
}
