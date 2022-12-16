#!/bin/bash

cat /dev/null > out.log

#for ((i = 0; i < 1; i++)); do
#  go test -run 2C -race >> out.log
#done

for ((i = 0; i < 5; i++));do
  go test -run 2D -race >> out.log
done

#for ((i = 0; i < 10; i++));do
#  go test -run TestFigure8Unreliable2C -race >> out.log
#done