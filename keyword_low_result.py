#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：keyword_low_result.py
#功能說明：檢索關鍵字查不到符合資源之關鍵字分析
#輸入參數：
#資料來源：searchlog
#輸出結果：marketanalysis -> keyword_low_result
#開發人員：Derek

from __future__ import division
from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
from urllib import *
from importlib import reload
import datetime, sys, urllib, pymysql
import string
from urllib.parse import quote

reload(sys)
sys.path.append("..")
from settings import *


conn = pymysql.connect(host=mysql_hostname, database=mysql_dbname, user=mysql_user, passwd=mysql_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
db_edumarket = client[mongo_dbname_edumarket]

lastLog_id = ''
ini_start_dt = ''
ini_end_dt = ''

## 如上次以跑過分析, 從最後一筆分析往後繼續跑, 避免重複浪費資源情況發生
is_lastlog_exists = db_marketanalysis.lastlog.find({'type': 'keyword_low_result'}).explain()['executionStats']['nReturned']  ## 是否存在lastId
if is_lastlog_exists == 1:
	lastLog_data = db_marketanalysis.lastlog.find({'type': 'keyword_low_result'})
	for lastLog in lastLog_data:
		lastLog_id = lastLog['id']
	last_datas = db_edumarket.searchlog.find({'_id': lastLog_id})
	for last_data in last_datas:
		ini_start_dt = last_data['since']
	end_date_data = db_edumarket.searchlog.find().sort('since', -1).limit(1);
	for end_date in end_date_data:
		lastLog_id = end_date['_id']
		ini_end_dt = end_date['since']
else:
	## 找出searchlog中開始與結束時間
	start_date_data = db_edumarket.searchlog.find().sort('since', 1).limit(1);	
	for start_date in start_date_data:
		ini_start_dt = start_date['since']
	end_date_data = db_edumarket.searchlog.find().sort('since', -1).limit(1);
	for end_date in end_date_data:
		lastLog_id = end_date['_id']
		ini_end_dt = end_date['since']

start_year = ini_start_dt.year
start_month = ini_start_dt.month
end_year = ini_end_dt.year
end_month = ini_end_dt.month

start_dt = datetime.datetime.strptime(str(start_year) + '-' + str(start_month), '%Y-%m')
end_dt = datetime.datetime.strptime(str(end_year) + '-' + str(end_month), '%Y-%m') + relativedelta(months=+1)  

while start_dt < end_dt:
	this_startDt = start_dt + datetime.timedelta(hours=-8)
	this_endDt = start_dt + relativedelta(days=+1) + datetime.timedelta(hours=-8)
	print(str(this_startDt) + ' - ' + str(this_endDt))

	keywordFreqList = db_edumarket.searchlog.aggregate(
	   [
		  {
			'$match' : {
			   'class_code': {'$in': ['Q','QSK']},
			   'since': {'$gte': this_startDt, '$lte': this_endDt}
			},
		  },
		  {
			'$group' : {
			   '_id' : { 'keyword' : '$sec_col' },
			   'count': { '$sum': 1 }
			}
		  },
		  { '$sort' : { 'count' : -1 } }
	   ]
	)

	topKeyword = {}
	topKeywordSet = {}
	for doc in list(keywordFreqList) :
		key = doc['_id']['keyword']
		topKeyword[key] = doc['count']

	lowerset = set(k.lower() for k in topKeyword)
	for kl in lowerset:
		for k, v in topKeyword.items():
			if kl == k.lower():
				topKeywordSet[kl] = v 
			elif kl == k.lower() and kl in topKeywordSet.keys():
				topKeywordSet[kl] = topKeywordSet[kl] + v


	low_result_list = []  ## 存放檢索結果次數少於3的關鍵字
	for keyword in topKeywordSet:
		try:
			url = solrServer + 'select?q=' + urllib.quote(keyword.encode('unicode_escape')) + '&wt=python&rows=20'
			connection = urllib.request.urlopen(quote(url, safe=string.printable))
			response = eval(connection.read())
			num_found = response['response']['numFound']
			if (num_found <= 3):
				low_result_list.append(keyword)
		except:
			continue

	year_month = str(this_endDt.year) + '-' + str(this_endDt.month)
	day = str(this_endDt.day)
	# print(json.dumps(low_result_list, ensure_ascii=False, encoding='UTF-8'))
	db_marketanalysis.keyword_low_result.update_one(
		{'year_month' : year_month, 'day': day},
		{'$set' : {'year_month' : year_month, 'day': day, 'keyword': low_result_list}},
		upsert=True
	)

	start_dt = this_endDt

db_marketanalysis.lastlog.update_one({'type': 'keyword_low_result'}, {'$set': {'id': lastLog_id}}, upsert=True)

client.close()
conn.close()