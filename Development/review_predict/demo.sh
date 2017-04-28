#!/bin/bash
spark-submit --master yarn-client --driver-memory 3G --executor-memory 3G --num-executors 5 --executor-cores 4 target/scala-2.11/review_predict_2.11-0.1-SNAPSHOT.jar
