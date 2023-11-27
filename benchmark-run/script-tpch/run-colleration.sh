#!/bin/bash

tid=${1:-9}
qid=${2:-1}


for k1 in 16g; do
  for k2 in 2 3 4 5 6 7 8; do
    for k3 in 4; do
      for s1 in 64MB; do
        for s4 in 10MB ; do
          for s5 in 8 12 16 20 24 28 32 36 40 48 56 64; do
            for i in 1 2 3; do
              name=tpch100_q${tid}-${qid}_k2:${k2}_k3:${k3}_s5:${s5}
              sync
              ssh node2 sync
              ssh node3 sync
              ssh node4 sync
              ssh node5 sync
              ssh node6 sync
              sleep 5
              echo -------------------------
              echo start running $name x$i
              bash run-tpch-q.sh -n $name -q "${tid} ${qid}" -c "${k1} ${k2} ${k3} $((k2 * k3 * 2)) 48m 200 true 0.6" -p "${s1} 0.2 0b ${s4} ${s5} 256MB 5.0 128MB 4MB" -x "outs/taskz" | tee logs/${name}_x${i}.log 2>&1
            done
          done
        done
      done
    done
  done
done


for k1 in 2g 4g 8g 16g 32g 64g 128g; do
  for k2 in 5; do
    for k3 in 4; do
      for s1 in 64MB; do
        for s4 in 10MB ; do
          for s5 in 200; do
            for s8 in 32g 64g 128g 256g 512g; do
              for i in 1 2 3; do
                name=tpch100_q${tid}-${qid}_k1:${k1}_k3:${k3}_s8:${s8}
                sync
                ssh node2 sync
                ssh node3 sync
                ssh node4 sync
                ssh node5 sync
                ssh node6 sync
                sleep 5
                echo -------------------------
                echo start running $name x$i
                bash run-tpch-q.sh -n $name -q "${tid} ${qid}" -c "${k1} ${k2} ${k3} $((k2 * k3 * 2)) 48m 200 true 0.6" -p "${s1} 0.2 0b ${s4} ${s5} 256MB 5.0 ${s8} 4MB" -x "outs/taskz" | tee logs/${name}_x${i}.log 2>&1
              done
            done
          done
        done
      done
    done
  done
done
