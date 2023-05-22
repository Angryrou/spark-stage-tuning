# Author(s): Chenghao Lyu <chenghao at cs dot umass dot edu>
# Created at 9/19/22

KITPATH=$1   # $PWD/resources/tpch-kit
QPT=$2       # query per template
SF=${3:-100} # SF

OUTPATH=${KITPATH}/spark-sqls
if [ -d "$KITPATH" ]; then
  ### Take action if $DIR exists ###
  echo "Generating 22 * ${QPT} = $((22 * QPT)) Spark SQLs in ${OUTPATH}"
else
  ###  Control will jump here if $KITPATH does NOT exists ###
  echo "Error: ${KITPATH} not found. Can not continue."
  exit 1
fi

cd "$KITPATH"
export DSS_CONFIG=${PWD}/dbgen
export DSS_QUERY=${DSS_CONFIG}/queries_sparksql

for t in {1..22}; do
  mkdir -p ${OUTPATH}/${t}
  for ((i = 1; i <= QPT; i++)); do
    dbgen/spark_wrapper.sh "qgen -r ${i} -s ${SF} ${t}" ${OUTPATH}/${t}/${t}-${i}.sql
  done
  echo "done for template $t"
done