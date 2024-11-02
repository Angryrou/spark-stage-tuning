# Author(s): Chenghao Lyu <chenghao at cs dot umass dot edu>
# Created at 9/19/22

KITPATH=$1   # $PWD/resources/tpcds-kit
QPT=$2       # query per template
SF=${3:-100} # SF

OUTPATH=${KITPATH}/spark-sqls-ext
if [ -d "$KITPATH" ]; then
  ### Take action if $DIR exists ###
  echo "Generating 1000 * ${QPT} = $((1000 * QPT)) Spark SQLs in ${OUTPATH}"
else
  ###  Control will jump here if $KITPATH does NOT exists ###
  echo "Error: ${KITPATH} not found. Can not continue."
  exit 1
fi

cd "$KITPATH"
#
#export DSS_CONFIG=${PWD}/tools
#export DSS_QUERY=${PWD}/query_templates_sparksql

sqls=$(seq 101 1100 | tr '\n' ' ')

cd tools
for q in $sqls; do
  mkdir -p ${OUTPATH}/${q}
  ./dsqgen \
  -DIRECTORY ../query_templates_sparksql_extend \
  -TEMPLATE T${q}.tpl \
  -SCALE $SF \
  -DIALECT spark \
  -VERBOSE Y \
  -STREAMS $QPT \
  -OUTPUT_DIR ../spark-sqls-ext/${q}

  for ((i = 1; i <= QPT; i++)); do
    mv ${OUTPATH}/${q}/query_$((i-1)).sql ${OUTPATH}/${q}/${q}-${i}.sql
  done

  echo "done for template $q"
done