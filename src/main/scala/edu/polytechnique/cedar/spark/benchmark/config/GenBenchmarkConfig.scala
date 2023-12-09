package edu.polytechnique.cedar.spark.benchmark.config

case class GenBenchmarkConfig(
    benchmarkName: String = null, // tpch / tpcds
    dataGenDir: String = "/mnt/disk7/chenghao-dataset",
    scaleFactor: String = null, // 1
    locationHeader: String = "hdfs://node13-opa:8020/user/spark_benchmark",
    overwrite: Boolean = false,
    databaseName: String = null
)
