package edu.polytechnique.cedar.spark.benchmark.config

case class RunJOBTemplateQueryConfig(
    benchmarkName: String = null, // tpch / tpcds
    queryLocationHeader: String = null,
    databaseName: String = "job",
    templateName: String = null,
    extractedPath: String = null,
    localDebug: Boolean = false,
    verbose: Boolean = false,
    enableRuntimeSolver: Boolean = false,
    runtimeSolverHost: String = "localhost",
    runtimeSolverPort: Int = 12345
)
