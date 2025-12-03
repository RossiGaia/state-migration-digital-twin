#!/bin/bash

for i in {1..5}; do
    curl http://localhost:5003/delta | curl -X POST http://localhost:5002/delta --json @-
    sleep 3
done