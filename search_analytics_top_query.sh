#!/bin/bash
# 取得Google Search Console中最近一個月內前5000筆的查詢詞,
# 並將其匯出成為前台標門標籤的網頁區塊

yesterdayDate=`date --date="yesterday" +%Y-%m-%d`
beforeMonthDate=`date --date='-1 month' +%Y-%m-%d`

python search_analytics_top_query.py 'https://market.cloud.edu.tw/' $beforeMonthDate $yesterdayDate --noauth_local_webserver
python export_topquery_hottag_normal.py
