#!/bin/bash
#統計會員成長資料
Lock="/tmp/membergrowing.lock"

if [ -e "$Lock" ];then
    echo "There is a process running, exit."
    exit 1
else
    touch $Lock

    cd /www/edumarket_batch2/analysis/bin
    python3.8 member_growing.py

    rm -f $Lock
    exit 0
fi
