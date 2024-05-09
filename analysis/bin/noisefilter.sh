#!/bin/bash
#進行log過濾雜訊的清理處理

Lock="/tmp/noisefilter.lock"

if [ -e "$Lock" ];then
    echo "There is a process running, exit."
    exit 1
else
    touch $Lock
    cd /www/edumarket_batch2/analysis/bin
    python3.8 noisefilter.py
    rm -f $Lock
    exit 0
fi
