#!/bin/bash

# Initialize variables with default values
query="9 1"
theta_c="1 1g 16 40 48m 200 true 0.6"
theta_p="64MB 0.2 0MB 10MB 200 256MB 5.0 128MB 4MB"
theta_s="0.2 1024KB"
name="unnamed"
xpath="./outs"
updateLqpId=""
verbose_mode=false

# Define usage function
usage() {
  echo "Usage: $0 -q <query> -c <theta_c> -p <theta_p> -s <theta_s> -n <name> -x <xpath> -i <updateLqpId>[-v]"
  echo "  -q <query>: Specify template id (tid) and query variant id (qid)."
  echo "  -c <theta_c>: Specify context parameters from k1 to k8"
  echo "  -p <theta_p>: Specify logical query plan (LQP) parameters from s1 to s7"
  echo "  -s <theta_s>: Specify query stage (QS) parameters from s8 to s9"
  echo "  -n <name>: Specify the name of spark app"
  echo "  -x <xpath>: Specify the path for the extracted traces"
  echo "  -i <updateLqpId>: Specify which AQE entry to update the runtime configuration "
  echo "  -v: Enable verbose mode."
  exit 1
}

# Parse command line options with getopts
while getopts "q:c:p:s:n:x:i:v" opt; do
  case "$opt" in
    q) query="$OPTARG";;
    c) theta_c="$OPTARG";;
    p) theta_p="$OPTARG";;
    s) theta_s="$OPTARG";;
    n) name="$OPTARG";;
    x) xpath="$OPTARG";;
    i) updateLqpId="$OPTARG";;
    v) verbose_mode=true;;
    \?) usage;;
  esac
done

read tid qid <<< "$query"
qsign=q${tid}-{qid}
read k1 k2 k3 k4 k5 k6 k7 k8 <<< "$theta_c"
read s1 s2 s3 s4 s5 s6 s7 s8 s9 <<< "$theta_p"
read s10 s11 <<< "$theta_s"

spath=/opt/hex_users/$USER/chenghao/spark-stage-tuning
jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
lpath=/opt/hex_users/$USER/chenghao/spark-stage-tuning/src/main/resources/log4j2.properties
qpath=/opt/hex_users/$USER/chenghao/UDAO2022

~/spark/bin/spark-submit \
--class edu.polytechnique.cedar.spark.benchmark.RunTemplateQueryForRuntimeTaskB \
--name ${name} \
--master yarn \
--deploy-mode client \
--conf spark.executorEnv.JAVA_HOME=${jpath} \
--conf spark.yarn.appMasterEnv.JAVA_HOME=${jpath} \
--conf spark.executor.cores=${k1} \
--conf spark.executor.memory=${k2} \
--conf spark.executor.instances=${k3} \
--conf spark.default.parallelism=${k4} \
--conf spark.reducer.maxSizeInFlight=${k5} \
--conf spark.shuffle.sort.bypassMergeThreshold=${k6} \
--conf spark.shuffle.compress=${k7} \
--conf spark.memory.fraction=${k8} \
--conf spark.yarn.am.cores=${k1} \
--conf spark.yarn.am.memory=${k2} \
--conf spark.sql.adaptive.enabled=true \
--conf spark.driver.maxResultSize=0 \
--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=${s1} \
--conf spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=${s2} \
--conf spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=${s3} \
--conf spark.sql.autoBroadcastJoinThreshold=${s4} \
--conf spark.sql.adaptive.autoBroadcastJoinThreshold=${s4} \
--conf spark.sql.shuffle.partitions=${s5} \
--conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=${s6} \
--conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=${s7} \
--conf spark.sql.files.maxPartitionBytes=${s8} \
--conf spark.sql.files.openCostInBytes=${s9} \
--conf spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor=${s10} \
--conf spark.sql.adaptive.coalescePartitions.minPartitionSize=${s11} \
--conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
--conf spark.sql.parquet.compression.codec=snappy \
--conf spark.sql.broadcastTimeout=10000 \
--conf spark.rpc.askTimeout=12000 \
--conf spark.shuffle.io.retryWait=60 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=512m \
--driver-java-options "-Dlog4j.configuration=file:$lpath" \
--conf "spark.driver.extraJavaOptions=-Xms20g" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files "$lpath" \
--jars ~/spark/examples/jars/scopt_2.12-3.7.1.jar \
$spath/target/scala-2.12/spark-stage-tuning_2.12-1.0-SNAPSHOT.jar \
-b tpch -t ${tid} -q ${qid} -s 100 -x ${xpath} -l ${qpath}/resources/tpch-kit/spark-sqls -i ${updateLqpId}
