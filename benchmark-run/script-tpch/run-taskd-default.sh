#!/bin/bash

tid=${1:-9}
qid=${2:-1}


for k1 in 8g 16g 32g; do
  for k2 in 3 5 ; do
    for k3 in 4 6 8 10; do
      for s1 in 16MB 64MB 256MB; do
        for s4 in 10MB 320MB ; do
          for s5 in 50 100 200 500; do
            for i in 1 2 3; do
              name=tpch100_q${tid}-${qid}_k1:${k1}_k2:${k2}_k3:${k3}_s1:${s1}_s4:${s4}_s5:${s5}
              sync
              ssh node2 sync
              ssh node3 sync
              ssh node4 sync
              ssh node5 sync
              ssh node6 sync
              sleep 5
              echo -------------------------
              echo start running $name x$i
              bash run-tpch-q.sh -n $name -q "${tid} ${qid}" -c "${k1} ${k2} ${k3} $((k2 * k3 * 2)) 48m 200 true 0.6" -p "${s1} 0.2 0b ${s4} ${s5} 256MB 5.0 128MB 4MB" -x "outs/taskd" | tee logs/${name}_x${i}.log 2>&1
            done
          done
        done
      done
    done
  done
done
