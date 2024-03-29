#!/bin/bash

tid=${1:-9}
qid=${2:-1}

# assertation that tid=5
if [ "$tid" -eq 5 ]; then
  echo "Variable 'tid' is equal to 5."
else
  echo "Variable 'tid' is not equal to 5."
  exit 1  # Exit with an error status
fi

s1s4s5="64MB 10MB 200"

# s1s4s5Worst1="256MB 10MB 100"
# s1s4s5Worst2="16MB 320MB 100"
# s1s4s5Worst3="16MB 10MB 500"

uId=1
read s1 s4 s5 <<< $s1s4s5
name=tpch100_q${tid}-${qid}_s1:${s1}_s4:${s4}_s5:${s5}_adapt_uId:${uId}
for i in 1 2 3; do
  sync
  ssh node2 sync
  ssh node3 sync
  ssh node4 sync
  ssh node5 sync
  ssh node6 sync
  sleep 5
  echo -------------------------
  echo start running $name x$i
  bash run-tpch-q-adapt.sh -n $name -q "${tid} ${qid}" -p "${s1} 0.2 0b ${s4} ${s5} 256MB 5.0 128MB 4MB" -x "outs/taskb" -i ${uId} | tee logs/${name}_x${i}.log 2>&1
done
