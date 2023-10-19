#!/bin/bash

tid=${1:-9}
qid=${2:-1}

s1s4s5Default="64MB 10MB 200"
s1s4s5Worst="256MB 10MB 200"

for s1s4s5 in "$s1s4s5Default" "$s1s4s5Worst"; do
  for uId in 1 2 3 4; do
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
      bash run-tpch-q-adapt.sh -n $name -q "${tid} ${qid}" -p "${s1} 0.2 0b ${s4} ${s5} 256MB 5.0" -x "outs/taskb" -i ${uId} | tee logs/${name}_x${i}.log 2>&1
    done
  done
done