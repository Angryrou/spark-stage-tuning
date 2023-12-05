package edu.polytechnique.cedar.spark.benchmark.config

case class RunTemplateQueryConfig(
    benchmarkName: String = null, // TPCH / TPCDS
    scaleFactor: String = null, // 1
    queryLocationHeader: String = null,
    databaseName: String = null,
    queryName: String = null,
    templateName: String = null,
    extractedPath: String = null,
    localDebug: Boolean = false,
    verbose: Boolean = false
)
