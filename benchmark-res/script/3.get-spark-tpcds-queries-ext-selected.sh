# Author(s): Chenghao Lyu <chenghao at cs dot umass dot edu>
# Created at 9/19/22

KITPATH=$1   # $PWD/resources/tpcds-kit
QPT=$2       # query per template
SF=${3:-100} # SF

OUTPATH=${KITPATH}/spark-sqls-ext-selected
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

#sqls=$(seq 101 1100 | tr '\n' ' ')
sqls="341 650 454 698 406 1071 729 1011 419 466 274 165 927 486 1069 1089 429 799 239 489 1024 1009 501 897 1021 982 597 356 310 763 987 104 101 600 721 340 1049 335 1084 725 992 1031 1079 989 912 269 952 330 174 326 607 816 400 665 1019 167 868 118 302 114 261 1099 142 626 757 199 266 1059 153 789 194 352 1091 534 1054 678 112 999 107 937 436 301 857 877 778 669 195 977 1094 979 617 577 206 932 687 967 560 229 470 661 592 1029 384 1044 1034 809 398 553 639 612 132 411 887 334 695 892 283 1074 1039 947 962 797 540 1041 1001 1051 922 508 1014 949 565 734 259 1061 847 785 155 869 703 629 441 819 102 717 376 959 806 972 837 957 185 369 969 296 360 939 904 929 1064 1004 917 942 919 901 744 529 145 997 133 1081 205 827 546 277"

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
  -OUTPUT_DIR ../spark-sqls-ext-selected/${q}

  for ((i = 1; i <= QPT; i++)); do
    mv ${OUTPATH}/${q}/query_$((i-1)).sql ${OUTPATH}/${q}/${q}-${i}.sql
  done

  echo "done for template $q"
done