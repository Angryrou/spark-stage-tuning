#!/bin/bash

tid=${1:-9}
qid=${2:-1}


for s1 in 16MB 64MB 256MB; do
  for s4 in 10MB 320MB ; do
    for s5 in 50 100 200 500; do
      for i in 1 2 3; do
        name=tpch100_q${tid}-${qid}_s1:${s1}_s4:${s4}_s5:${s5}
        sync
        ssh node2 sync
        ssh node3 sync
        ssh node4 sync
        ssh node5 sync
        ssh node6 sync
        sleep 5
        echo -------------------------
        echo start running $name x$i
        bash run-tpch-q.sh -n $name -q "${tid} ${qid}" -p "${s1} 0.2 0b ${s4} ${s5} 256MB 5.0" -x "outs/taskb" | tee logs/${name}_x${i}.log 2>&1
      done
    done
  done
done
