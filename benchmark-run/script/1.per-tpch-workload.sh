s1=$1
s2=$2
s3=$3
s4=$4
trial=$5
loc=$6

for t in {1..22}
do
  qsign="q${t}-1"
  spath=/opt/hex_users/$USER/chenghao/spark-stage-tuning
  jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
  lpath=/opt/hex_users/$USER/chenghao/spark-stage-tuning/src/main/resources/log4j2.properties

  name=TPCH100_PER_BM_${qsign}_${s1}_${s2}_${s3}_${s4}
  ~/spark/bin/spark-submit \
  --class edu.polytechnique.cedar.spark.sql.benchmark.RunTemplateQuery \
  --name ${name} \
  --master yarn \
  --deploy-mode client \
  --conf spark.executorEnv.JAVA_HOME=${jpath} \
  --conf spark.yarn.appMasterEnv.JAVA_HOME=${jpath} \
  --conf spark.executor.memory=16g \
  --conf spark.executor.cores=5 \
  --conf spark.executor.instances=4 \
  --conf spark.default.parallelism=40 \
  --conf spark.reducer.maxSizeInFlight=48m \
  --conf spark.shuffle.sort.bypassMergeThreshold=200 \
  --conf spark.shuffle.compress=true \
  --conf spark.memory.fraction=0.6 \
  --conf spark.sql.inMemoryColumnarStorage.batchSize=10000 \
  --conf spark.sql.files.maxPartitionBytes=128MB \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
  --conf spark.yarn.am.cores=5 \
  --conf spark.yarn.am.memory=16g \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=${s1} \
  --conf spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=${s2} \
  --conf spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=${s3} \
  --conf spark.sql.autoBroadcastJoinThreshold=${s4} \
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
  -b TPCH -t $t -q 1 -s 100 -l resources/tpch-kit/spark-sqls > ${loc}/${name}_x${trial}.log 2>&1
done