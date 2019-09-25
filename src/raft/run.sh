#!/usr/bin/env bash

max_i=$1
max_j=$2
test_name=$3

rm -f TestRet*

for ((i=0;i<$max_i;i++)); do
    for ((j=0;j<$max_j;j++)); do
        go test -race -v -run $test_name > TestRet${i}_${j} &
    done
    go test -race -v -run $test_name > TestRet${i}_${j}
done
