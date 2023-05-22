# spark-benchmark-setup-local

We show how we get enough dependencies to set up the benchmark trace collection, including

## Get Dependencies

1. the packaged jar in [spark-sql-perf][1] derived from [databricks][2]
    ```bash
    # (optional) already prepared in benchmark-res/libs
    cd benchmark-res/script
    bash 1.get-spark-sql-perf.sh
    ```

2. the executable object compiled in [tpch-kit][3] 
   ```bash
   # example in MACOS
   cd benchmark-res/script
   bash 2.get-spark-tpch-kit-local.sh MACOS
   ```

3. the executable object compiled in [tpcds-kit][4]
   ```bash
   # example in MACOS
   cd benchmark-res/script
   bash 2.get-spark-tpcds-kit-local.sh MACOS
   ```

## Download TPC-benchmark Dataset

1. get the TPCH dataset
   ```bash
   DIR=/Users/chenghao/ResearchHub/repos/spark-stage-tuning
   SPARK_PATH=/Users/chenghao/ResearchHub/repos/spark/dist
   SF=1
   ${SPARK_PATH}/bin/spark-submit \
   --master local[*] \
   --deploy-mode client \
   --class edu.polytechnique.cedar.spark.benchmark.SetBenchmark \
   --name set-benchmark \
   --conf spark.driver.cores=5 \
   --conf spark.driver.memory=4g \
   --conf spark.default.parallelism=40 \
   --conf spark.sql.adaptive.enabled=true \
   --conf spark.sql.parquet.compression.codec=snappy \
   --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
   --conf spark.kryoserializer.buffer.max=512m \
   --conf spark.driver.extraClassPath=file://${DIR}/benchmark-res/libs/mysql-connector-j-8.0.33.jar \
   --jars file://${SPARK_PATH}/examples/jars/scopt_2.12-3.7.1.jar,file://${DIR}/benchmark-res/libs/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
   ${DIR}/target/scala-2.12/spark-stage-tuning_2.12-1.0-SNAPSHOT.jar \
   -b TPCH -d ${DIR}/benchmark-res/dataset-gen/tpch-kit -s $SF -l hdfs://localhost:8020/user/spark_benchmark
   ```

2. get the TPCDS dataset
   ```bash
   DIR=/Users/chenghao/ResearchHub/repos/spark-stage-tuning
   SPARK_PATH=/Users/chenghao/ResearchHub/repos/spark/dist
   SF=1
   ${SPARK_PATH}/bin/spark-submit \
   --master local[*] \
   --deploy-mode client \
   --class edu.polytechnique.cedar.spark.benchmark.SetBenchmark \
   --name set-benchmark \
   --conf spark.driver.cores=5 \
   --conf spark.driver.memory=4g \
   --conf spark.default.parallelism=40 \
   --conf spark.sql.adaptive.enabled=true \
   --conf spark.sql.parquet.compression.codec=snappy \
   --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
   --conf spark.kryoserializer.buffer.max=512m \
   --conf spark.driver.extraClassPath=file://${DIR}/benchmark-res/libs/mysql-connector-j-8.0.33.jar \
   --jars file://${SPARK_PATH}/examples/jars/scopt_2.12-3.7.1.jar,file://${DIR}/benchmark-res/libs/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
   ${DIR}/target/scala-2.12/spark-stage-tuning_2.12-1.0-SNAPSHOT.jar \
   -b TPCDS -d ${DIR}/benchmark-res/dataset-gen/tpcds-kit -s $SF -l hdfs://localhost:8020/user/spark_benchmark   
   ```

## Generate Queries

1. get TPCH queries
   ```bash
   # local
   bash benchmark-res/script/2.get-spark-tpch-queries.sh  $PWD/benchmark-res/dataset-gen/tpch-kit 3 1   
   # Ercilla
   bash benchmark-res/script/2.get-spark-tpch-queries.sh  $PWD/benchmark-res/dataset-gen/tpch-kit 4545 
   ```

2. get TPCDS queries
   ```bash
   # local
   bash benchmark-res/script/3.get-spark-tpcds-queries.sh  $PWD/benchmark-res/dataset-gen/tpcds-kit 3 1
   # Ercilla
   bash benchmark-res/script/3.get-spark-tpcds-queries.sh  $PWD/benchmark-res/dataset-gen/tpcds-kit 971
   ```
   
[1]: https://github.com/Angryrou/spark-sql-perf
[2]: https://github.com/databricks/spark-sql-perf
[3]: https://github.com/Angryrou/tpch-kit
[4]: https://github.com/databricks/tpcds-kit

