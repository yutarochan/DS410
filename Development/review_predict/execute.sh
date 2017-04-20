#!/bin/bash
# Executor Parameter Space
nodes=( 4 5 )
cores=( 16 )

# Iterate Executions
for i in "${nodes[@]}"
do
    for j in "${cores[@]}"
    do
        spark-submit --master yarn-client --driver-memory 15g --executor-memory 15g --num-executors $i --executor-cores $j target/scala-2.11/review_predict_2.11-0.1-SNAPSHOT.jar
    done
done
