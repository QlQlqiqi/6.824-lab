#!/bin/bash

for ((i = 0; i < 10; i++)); do
  go test -run 2C -race >> out.log
done
