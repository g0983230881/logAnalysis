#!/bin/bash
#執行資源使用情況的時序性分析

Lock="/tmp/resources_timeline.lock"

if [ -e "$Lock" ];then
    echo "There is a process running, exit." 
    exit 1
else
    touch $Lock
    cd /www/edumarket_batch2/analysis/bin

    python resources_timeline.py

    rm -f $Lock
    exit 0
fi

