#!/bin/bash

cat /dev/null > out.log

for ((i = 0; i < 5; i++)); do
  go test -race >> out.log
done