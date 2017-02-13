#!/usr/bin/env bash

# Setup for Chapter 4 Decision Forest example
hdfs dfs -mkdir /tmp/Covtype
curl https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz | gunzip | hdfs dfs -put - /tmp/Covtype/covtype.data

# Setup for Chapter 5 clustering example
hdfs dfs -mkdir /tmp/KDDCup
curl http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz | gunzip | hdfs dfs -put - /tmp/KDDCup/kddcup.data
