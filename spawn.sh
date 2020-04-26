#!/bin/bash

for i in `seq 8500 8510`; do
  mkdir data/$i
  node server.js $i data/$i/ &
done

# ps -ef | grep "node server.js" | awk '{print $2}' | xargs kill -9