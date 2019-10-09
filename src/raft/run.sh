#!/usr/bin/env bash

source ~/.profile
export GOPATH=$GOPATH:/home/maichao/Documents/codes/mit-6.824

max_i=$1
max_j=$2
test_name=$3

rm -f TestRet*
rm -f raft.test
go test -race -c

for ((i=0;i<$max_i;i++)); do
    for ((j=0;j<$max_j;j++)); do
        ./raft.test -test.v -test.run $test_name > TestRet${i}_${j} &
    done
    ./raft.test -test.v -test.run $test_name > TestRet${i}_${j}
done
