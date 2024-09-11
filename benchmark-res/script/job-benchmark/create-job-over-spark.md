Steps to set up Join-order Benchmark over Spark.
---

## Download IMDB and upload to HDFS
```bash
# download
wget http://homepages.cwi.nl/~boncz/job/imdb.tgz
tar -xvzf imdb.tgz
mkdir csv
mv *.csv csv

# move data to hdfs
hdfs dfs -put csv hdfs://node1-opa:8020/user/spark_benchmark/job-csv
```

## Setup `job` Database over Spark

### Step 1: start pyspark (can use spark-shell as well)

```bash
# start pyspark
jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
~/spark/bin/pyspark   --master yarn   --deploy-mode client   --conf spark.executorEnv.JAVA_HOME=$jpath   --conf spark.yarn.appMasterEnv.JAVA_HOME=$jpath   --conf spark.default.parallelism=100   --conf spark.executor.instances=20   --conf spark.executor.cores=5   --conf spark.executor.memory=80g   --conf spark.driver.memory=80g   --conf spark.reducer.maxSizeInFlight=256m   --conf spark.rpc.askTimeout=12000   --conf spark.shuffle.io.retryWait=60   --conf spark.sql.autoBroadcastJoinThreshold=200m   --conf "spark.driver.extraJavaOptions=-Xms20g"   --jars /opt/hex_users/$USER/chenghao/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
```

### Step 2: do the magic within python-shell

```python
# Create database
sql("create database job")
sql("use job")

sql_file_path = "./schematext-spark.sql"  # Replace with your actual file path
with open(sql_file_path, 'r') as file:
    sql_commands = file.read()
commands = sql_commands.split(";")
for command in commands:
    trimmed_command = command.strip()
    if trimmed_command:
        spark.sql(trimmed_command)

# Load data
hdfs_csv_path = "hdfs://node1-opa:8020/user/spark_benchmark/job-csv"
hdfs_parquet_path = "hdfs://node1-opa:8020/user/spark_benchmark/job"
tables = ['aka_name', 'aka_title', 'cast_info', 'char_name', 'company_name', 'company_type', 'comp_cast_type', 'complete_cast', 'info_type', 'keyword', 'kind_type', 'link_type', 'movie_companies', 'movie_info', 'movie_info_idx', 'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title']
for table in tables:
  print(f"working on {table}")
  df_hive = spark.sql(f"SELECT * FROM {table} WHERE 1=0")
  csv_file_path = f"{hdfs_csv_path}/{table}.csv"
  df = spark.read.csv(csv_file_path, schema=df_hive.schema, header=False)
  parquet_file_path = f"{hdfs_parquet_path}/{table}"
  df.write.mode('overwrite').parquet(parquet_file_path)
  
sql("drop database job cascade")
sql("create database job")
sql("use job")

for table in tables:
  parquet_file_path = f"{hdfs_parquet_path}/{table}"
  sqlContext.sparkSession.catalog.createTable(f"job.{table}", parquet_file_path, "parquet")

# Analyse all columns of all tables
def analyze_table(database_name: str, table: str, analyze_columns: bool = False):
    """
    Analyze a given table and optionally analyze each column for statistics.
    :param database_name: The name of the database in the Hive metastore
    :param table: The name of the table to analyze
    :param analyze_columns: Boolean flag to indicate if columns should be analyzed
    """
    # Analyze the table (compute table-level statistics)
    print(f"Analyzing table {table}.")
    sql(f"ANALYZE TABLE {database_name}.{table} COMPUTE STATISTICS")
    if analyze_columns:
        # Retrieve the schema to get column names
        df = sql(f"SELECT * FROM {database_name}.{table} LIMIT 0")
        column_names = ", ".join(df.columns)
        # Analyze columns (compute column-level statistics)
        print(f"Analyzing table {table} columns {column_names}.")
        sql(f"ANALYZE TABLE {database_name}.{table} COMPUTE STATISTICS FOR COLUMNS {column_names}")

# Example usage
database_name = "job"
tables = [
    'aka_name', 'aka_title', 'cast_info', 'char_name', 'company_name',
    'company_type', 'comp_cast_type', 'complete_cast', 'info_type', 'keyword',
    'kind_type', 'link_type', 'movie_companies', 'movie_info', 'movie_info_idx',
    'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title'
]

# Loop over tables and analyze each one
for table in tables:
    analyze_table(database_name, table, analyze_columns=True)
```
