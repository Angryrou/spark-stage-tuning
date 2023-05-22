# Author(s): Chenghao Lyu <chenghao at cs dot umass dot edu>
# Created at 9/19/22

KITPATH=$1   # $PWD/resources/tpcds-kit
QPT=$2       # query per template
SF=${3:-100} # SF

if [ -d "$KITPATH" ]; then
  ### Take action if $DIR exists ###
  echo "Generating 103 * ${QPT} = $((103 * QPT)) Spark SQLs in ${OUTPATH}"
else
  ###  Control will jump here if $KITPATH does NOT exists ###
  echo "Error: ${KITPATH} not found. Can not continue."
  exit 1
fi

cd "$KITPATH"
OUTPATH=${KITPATH}/spark-sqls
#
#export DSS_CONFIG=${PWD}/tools
#export DSS_QUERY=${PWD}/query_templates_sparksql


sqls="1 2 3 4 5 6 7 8 9 10 11 12 13 14a 14b 15 16 17 18 19 20 21 22 23a 23b 24a 24b 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39a 39b 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99"

cd tools
for q in $sqls; do
  mkdir -p ${OUTPATH}/${q}
  ./dsqgen \
  -DIRECTORY ../query_templates_sparksql \
  -TEMPLATE query${q}.tpl \
  -SCALE $SF \
  -DIALECT spark \
  -VERBOSE Y \
  -STREAMS $QPT \
  -OUTPUT_DIR ../spark-sqls/${q}

  for ((i = 1; i <= QPT; i++)); do
    mv ${OUTPATH}/${q}/query_$((i-1)).sql ${OUTPATH}/${q}/${q}-${i}.sql
  done

  echo "done for template $q"
done