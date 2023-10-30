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

po_configs=(
  "8g 3 6 16MB 10MB 100"
  "8g 5 4 16MB 10MB 100"
  "8g 3 8 16MB 10MB 100"
  "8g 3 10 16MB 10MB 100"
  "8g 5 8 16MB 10MB 100"
  "8g 5 10 16MB 10MB 100"
)

uId=1
for po_config in "${po_configs[@]}"; do
  read k1 k2 k3 s1 s4 s5 <<< $po_config
  for i in 1 2 3; do
    name=tpch100_q${tid}-${qid}_k1:${k1}_k2:${k2}_k3:${k3}_s1:${s1}_s4:${s4}_s5:${s5}_over_adapt
    sync
    ssh node2 sync
    ssh node3 sync
    ssh node4 sync
    ssh node5 sync
    ssh node6 sync
    sleep 5
    echo -------------------------
    echo start running $name x$i
    bash run-tpch-q-adapt.sh -n $name -q "${tid} ${qid}" -c "${k1} ${k2} ${k3} $((k2 * k3 * 2)) 48m 200 true 0.6" -p "${s1} 0.2 0b ${s4} ${s5} 256MB 5.0" -x "outs/taskd" -i ${uId} | tee logs/${name}_x${i}.log 2>&1
  done
done