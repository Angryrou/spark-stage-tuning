package edu.polytechnique.cedar.spark.benchmark.config

case class RunTemplateQueryConfigTaskB(
    benchmarkName: String = null, // tpch / tpcds
    scaleFactor: String = null, // 1
    queryLocationHeader: String = null,
    databaseName: String = null,
    queryName: String = null,
    templateName: String = null,
    extractedPath: String = null,
    updateLqpId: String = null,
    localDebug: Boolean = false
)
