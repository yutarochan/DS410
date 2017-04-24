#!/bin/bash
# Executor Parameter Space
nodes=( 3 4 5 )
cores=( 2 4 8 )
mem=( 2 5 10 )

# Iterate Executions
for i in "${nodes[@]}"
do
    for j in "${cores[@]}"
    do
        for k in "${mem[@]}"
        do
            spark-submit --master yarn-client --driver-memory $k\G --executor-memory $k\G --num-executors $i --executor-cores $j target/scala-2.11/review_predict_2.11-0.1-SNAPSHOT.jar
        done
    done
done
