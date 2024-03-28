#!/bin/bash

cd /www/edumarket_batch2/analysis/bin
python noisefilter.py

python login_analysis.py
python ContributionAnalysis.py
python member_behaviorlog.py
python resources_usage.py
python resources_usagebehavior.py
python member_return.py

