#!/bin/bash

cat /dev/null > out.log

for ((i = 0; i < 5; i++)); do
  go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B -race >> out.log
done