#!/bin/bash
#進行會員操作行為log資料的分析

Lock="/tmp/loginanalysis.lock"

if [ -e "$Lock" ];then
    echo "There is a process running, exit."
    exit 1
else
    touch $Lock
    cd /www/edumarket_batch2/analysis/bin
    #python noisefilter.py

    python3.8 login_analysis.py
    python3.8 ContributionAnalysis.py
    python3.8 member_behaviorlog.py
    python3.8 resources_usage.py
    python3.8 resources_usagebehavior.py
    python3.8 member_return.py

    rm -f $Lock
    exit 0
fi
