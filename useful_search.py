#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：useful_search.py
#功能說明：分析使用者檢索到所需資源的比率
#輸入參數：
#資料來源：searchlog
#輸出結果：marketanalysis -> succeed_search
#開發人員：Derek

from __future__ import division
from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime, sys

sys.path.append("..")
from settings import *

client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
db_edumarket = client[mongo_dbname_edumarket]


lastLog_id = ''
ini_start_dt = ''
ini_end_dt = ''

is_lastlog_exists = db_marketanalysis.lastlog.find({'type': 'useful_search'}).explain()['executionStats']['nReturned']  ## 是否存在lastId
if is_lastlog_exists == 1:
	lastLog_data = db_marketanalysis.lastlog.find({'type': 'useful_search'})
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
# start_dt = datetime.datetime.strptime('2019-12', '%Y-%m')
# end_dt = datetime.datetime.strptime('2020-1', '%Y-%m')

while start_dt < end_dt:
	this_startDt = start_dt + datetime.timedelta(hours=-8)
	this_endDt = start_dt + relativedelta(days=+1) + datetime.timedelta(hours=-8)
	print(str(this_startDt) + ' - ' + str(this_endDt))
	
	success_count = 0
	fail_count = 0
	session_data = db_edumarket.searchlog.find({'class_code': {'$in': ['Q', 'QSK']}, 'since':{'$gte': this_startDt, '$lte': this_endDt}}).distinct('session_id')
	for session_id in session_data:
		log_datas = db_edumarket.searchlog.find({'session_id': session_id})

		tmp_code = ''  ## 暫存class_code以判斷 下一個行為是否為檢索成功
		for log_data in log_datas:
			class_code = log_data['class_code'] 
			sec_col = log_data['sec_col'] 
			
			if tmp_code == 'Q' or tmp_code == 'QSK':
				if class_code == 'R' or class_code == 'F':
					success_count += 1
				else:
					fail_count += 1
			tmp_code = class_code

	year_month = str(this_endDt.year) + '-' + str(this_endDt.month)
	day = str(this_endDt.day)
	succeed_search_per = 0
	try:
		succeed_search_per = round(success_count / (success_count + fail_count)*100, 2)
	except:
		succeed_search_per = 0

	db_marketanalysis.succeed_search.update_one(
		{'year_month' : year_month, 'day': day},
		{'$set' : {'year_month' : year_month, 'day': day, 'succeed_per': succeed_search_per, 'succeed_count': success_count, 'failure_count' : fail_count}},
		upsert=True
	)

	start_dt = this_endDt

db_marketanalysis.lastlog.update_one({'type': 'succeed_search'}, {'$set': {'id': lastLog_id}}, upsert=True)

client.close()