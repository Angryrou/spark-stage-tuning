package edu.polytechnique.cedar.spark.benchmark

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpch.TPCHTables
import com.databricks.spark.sql.perf.Tables
import org.apache.spark.sql.SparkSession

case class GenBenchmarkConfig(
    benchmarkName: String = null, // TPCH / TPCDS
    dataGenDir: String = "/mnt/disk7/chenghao-dataset",
    scaleFactor: String = null, // 1
    locationHeader: String = "hdfs://node13-opa:8020/user/spark_benchmark",
    overwrite: Boolean = false,
    databaseName: String = null
)

object SetBenchmark {

  val format = "Parquet"
  val TPCDSUseLegacyOptions = false
  val useDoubleForDecimal = false
  val useStringForDate = false
  val partitionTables = true
  val clusterByPartitionColumns = true
  val filterOutNullPartitionValues = false
  val defaultNumPartitions = 100
  val numPartitioned = 10000
  val numNonpartitioned = 10

  def isPartitioned(tables: Tables, tableName: String): Boolean =
    util
      .Try(
        tables.tables.find(_.name == tableName).get.partitionColumns.nonEmpty
      )
      .getOrElse(false)

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis() //nanoTime()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis() //nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  def main(args: Array[String]): Unit = {
    val parser =
      new scopt.OptionParser[GenBenchmarkConfig]("Gen-Benchmark-Data") {
        opt[String]('b', "benchmark")
          .action { (x, c) => c.copy(benchmarkName = x) }
          .text("the name of the benchmark to run")
          .required()
        opt[String]('d', "dataGenDir")
          .action { (x, c) => c.copy(dataGenDir = x) }
          .text("head dir of dsdgen/dbgen")
          .required()
        opt[String]('s', "scaleFactor")
          .action((x, c) => c.copy(scaleFactor = x))
          .text(
            "scaleFactor defines the size of the dataset to generate (in GB)"
          )
        opt[String]('l', "locationHeader")
          .action((x, c) => c.copy(locationHeader = x))
          .text("head root directory of location to create data in")
        opt[Boolean]('o', "overwrite")
          .action((x, c) => c.copy(overwrite = x))
          .text("overwrite the data that is already there")
        opt[String]('n', "databaseName")
          .action((x, c) => c.copy(databaseName = x))
          .text("customized databaseName")
        help("help")
          .text("prints this usage text")
      }

    parser.parse(args, GenBenchmarkConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: GenBenchmarkConfig): Unit = {
    val sf = config.scaleFactor

    val spark = SparkSession
      .builder()
      .config(
        "spark.sql.shuffle.partitions",
        if (sf.toInt >= 10000) "20000"
        else if (sf.toInt >= 1000) "2001"
        else "200"
      )
      .config(
        "parquet.memory.pool.ratio",
        if (sf.toInt >= 10000) "0.1" else if (sf.toInt >= 1000) "0.3" else "0.5"
      )
      .enableHiveSupport()
      .getOrCreate()

    val tables = config.benchmarkName match {
      case "TPCDS" => (
        new TPCDSTables(
          spark.sqlContext,
          dsdgenDir = s"${config.dataGenDir}/tools",
          scaleFactor = config.scaleFactor,
          useDoubleForDecimal = useDoubleForDecimal,
          useStringForDate = useStringForDate
        )
      )
      case "TPCH" => (
        new TPCHTables(
          spark.sqlContext,
          dbgenDir = s"${config.dataGenDir}/dbgen",
          scaleFactor = config.scaleFactor,
          useDoubleForDecimal = useDoubleForDecimal,
          useStringForDate = useStringForDate,
          generatorParams = Nil
        )
      )
    }

    val databaseName =
      if (config.databaseName == null)
        s"${config.benchmarkName.toLowerCase}_${config.scaleFactor}"
      else config.databaseName
    val location = s"${config.locationHeader}/${databaseName}/dataset"
    val workers =
      spark.sparkContext.getConf.get("spark.executor.instances", "1").toInt
    val cores = spark.sparkContext.getConf
      .get(
        "spark.executor.cores",
        spark.sparkContext.getConf.get("spark.driver.cores")
      )
      .toInt

    // name of database to create.

    val nonPartitionedTables =
      if (config.benchmarkName == "TPCDS")
        Array(
          "call_center",
          "catalog_page",
          "customer",
          "customer_address",
          "customer_demographics",
          "date_dim",
          "household_demographics",
          "income_band",
          "item",
          "promotion",
          "reason",
          "ship_mode",
          "store",
          "time_dim",
          "warehouse",
          "web_page",
          "web_site"
        )
      else
        Array(
          "nation",
          "region"
        )

    nonPartitionedTables.foreach { t =>
      {
        tables.genData(
          location = location,
          format = format,
          overwrite = config.overwrite,
          partitionTables = partitionTables,
          clusterByPartitionColumns = clusterByPartitionColumns,
          filterOutNullPartitionValues = filterOutNullPartitionValues,
          tableFilter = t,
          numPartitions = numNonpartitioned
        )
      }
    }

    val partitionedTables =
      if (config.benchmarkName == "TPCDS")
        Array(
          "inventory",
          "web_returns",
          "catalog_returns",
          "store_returns",
          "web_sales",
          "catalog_sales",
          "store_sales"
        )
      else
        Array("part", "supplier", "partsupp", "customer", "orders", "lineitem")

    partitionedTables.foreach { t =>
      {
        tables.genData(
          location = location,
          format = format,
          overwrite = config.overwrite,
          partitionTables = partitionTables,
          clusterByPartitionColumns = clusterByPartitionColumns,
          filterOutNullPartitionValues = filterOutNullPartitionValues,
          tableFilter = t,
          numPartitions =
            (workers * cores * 4).max(sf.toInt.min(numPartitioned))
        )
      }
    }

//    val tableNames = tables.tables.map(_.name)
//    tableNames.foreach { tableName =>
//      // generate data
//      time {
//        tables.genData(
//          location = location,
//          format = format,
//          overwrite = config.overwrite,
//          partitionTables = partitionTables,
//          // if to coalleace into a single file (only one writter for non partitioned tables = slow)
//          clusterByPartitionColumns = clusterByPartitionColumns, //if (isPartitioned(tables, tableName)) false else true,
//          filterOutNullPartitionValues = filterOutNullPartitionValues,
//          tableFilter = tableName,
//          // this controls parallelism on datagen and number of writers (# of files for non-partitioned)
//          // in general we want many writers to S3, and smaller tasks for large scale factors to avoid OOM and shuffle errors
//          numPartitions = if (sf.toInt <= 100 || !isPartitioned(tables, tableName)) 10 else min(sf.toInt, 10000)
//      }
//    }

    spark.sql(s"drop database if exists $databaseName cascade")
    println(s"Creating external tables at $location")
    tables.createExternalTables(
      location,
      format,
      databaseName,
      overwrite = true,
      discoverPartitions = true
    )
    spark.sql(s"use $databaseName")
    tables.analyzeTables(databaseName, analyzeColumns = true)

  }
}
