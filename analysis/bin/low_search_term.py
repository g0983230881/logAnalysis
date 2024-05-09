#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：low_search_term.py
#功能說明：分析每個月的全文檢索詞, 檢索結果較低頻率者
#輸入參數：無
#資料來源：edumarket -> searchterm
#輸出結果：marketanalysis -> low_search_term
#開發人員：Derek

from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime
import sys

sys.path.append("..")
from settings import *

client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
db_edumarket = client[mongo_dbname_edumarket]

min_date = ''
lastLog_id = ''
is_lastlog_exists = db_marketanalysis.lastlog.find({'type': 'low_search_term'}).explain()['executionStats']['nReturned']  ## 是否存在lastId
if is_lastlog_exists == 1:
	lastLog_data = db_marketanalysis.lastlog.find({'type': 'low_search_term'})
	for lastLog in lastLog_data:
		lastLog_id = lastLog['id']

	last_datas = db_edumarket.searchterm.find({'_id': lastLog_id})
	for last_data in last_datas:
		min_date = last_data['created_at'].strftime("%Y-%m")
		print(min_date)
else:
	end_date_data = db_edumarket.searchterm.find().sort('created_at', -1).limit(1);
	for end_date in end_date_data:
		lastLog_id = end_date['_id']
	min_date = '2020-04'

nowdate = datetime.datetime.now()
while (nowdate.strftime("%Y-%m") >= min_date) :
	print(str(nowdate.strftime("%Y-%m")) + '-' + min_date)
	this_month_users = []
	this_month_sessions = []

	searchterms = db_edumarket.searchterm.find({'created_at': {'$gte': (nowdate - relativedelta(month=1) + datetime.timedelta(hours=-8)), '$lt': nowdate + datetime.timedelta(hours=-8)}})
	for searchterm in searchterms:
		if searchterm['numfound'] < 1:
			member_id = searchterm['member_id']
			session_id = searchterm['session_id']
			if (member_id == -1):
				this_month_sessions.append(session_id)
			else:
				this_month_users.append(member_id)

	## 該月份檢索結果低頻率之使用者清單
	this_month_users = list(dict.fromkeys(this_month_users))
	this_month_sessions = list(dict.fromkeys(this_month_sessions))

	## 有紀錄會員編號
	for this_month_user in this_month_users:
		keyword_list = []
		extra_list = []
		fi_keyword_list = []
		fi_extra_list = []

		searchterms = db_edumarket.searchterm.find({'created_at': {'$gte': (nowdate - relativedelta(month=1) + datetime.timedelta(hours=-8)), '$lt': nowdate + datetime.timedelta(hours=-8)}, 'member_id' : this_month_user})
		for searchterm in searchterms:
			keyword_list.append(searchterm['keyword'])

		keyword_list = list(dict.fromkeys(keyword_list))
		for keyword in keyword_list:
			keFreqList = db_edumarket.searchterm.aggregate(
			  [
				 {
				   '$match' : {
					   'member_id': this_month_user,
					   'keyword': keyword
				   },
				 },
				 {
				   '$group' : {
					  '_id' : { 'extra' : "$extra" },
					  'count': { '$sum': 1 }
				   }
				 },
				 { '$sort' : { 'count' : -1 } }
			  ]
			)

			for doc in list(keFreqList) :
				fi_keyword_list.append(keyword)
				fi_extra_list.append(doc['_id']['extra'])

		for i in range(len(fi_keyword_list)):
			db_marketanalysis.low_search_term.update_one(
				{'user_id': this_month_user, 'year_month': nowdate.strftime("%Y-%m")},
				{'$set': {'user_id': this_month_user, 'keyword': fi_keyword_list[i], 'extra': fi_extra_list[i], 'year_month': nowdate.strftime("%Y-%m")}},
				upsert=True
			)

	## 有紀錄session編號
	for this_month_session in this_month_sessions:
		keyword_list = []
		extra_list = []
		fi_keyword_list = []
		fi_extra_list = []

		searchterms = db_edumarket.searchterm.find({'created_at': {'$gte': (nowdate - relativedelta(month=1) + datetime.timedelta(hours=-8)), '$lt': nowdate + datetime.timedelta(hours=-8)}, 'session_id' : this_month_session})
		for searchterm in searchterms:
			keyword_list.append(searchterm['keyword'])
		keyword_list = list(dict.fromkeys(keyword_list))

		for keyword in keyword_list:
			keFreqList = db_edumarket.searchterm.aggregate(
			  [
				 {
				   '$match' : {
					   'session_id': this_month_session,
					   'keyword': keyword
				   },
				 },
				 {
				   '$group' : {
					  '_id' : { 'extra' : "$extra" },
					  'count': { '$sum': 1 }
				   }
				 },
				 { '$sort' : { 'count' : -1 } }
			  ]
			)

			for doc in list(keFreqList) :
				fi_keyword_list.append(keyword)
				fi_extra_list.append(doc['_id']['extra'])

		for i in range(len(fi_keyword_list)):
			db_marketanalysis.low_search_term.update_one(
				{'session_id': this_month_session, 'year_month': nowdate.strftime("%Y-%m")},
				{'$set': {'session_id': this_month_session, 'keyword': fi_keyword_list[i], 'extra': fi_extra_list[i], 'year_month': nowdate.strftime("%Y-%m")}},
				upsert=True
			)

	nowdate = nowdate + relativedelta(months=-1)

db_marketanalysis.lastlog.update_one({'type': 'low_search_term'}, {'$set': {'id': lastLog_id}}, upsert=True)	

client.close()

