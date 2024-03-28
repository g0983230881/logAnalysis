#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：
#功能說明：
#輸入參數：
#資料來源：
#輸出結果：
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
import os,sys
import pymysql

sys.path.append("..")
from settings import *


#取得上次處理log的最後ObjectId
def getLastlogId():
	lastLogId = ""
	lastlog_data = db_marketanalysis.lastlog.find_one({'type': 'visitanalysis_lastId'})
	try:
		lastLogId = lastlog_data['id']
	except:
		lastLogId = ""
	return lastLogId

#conn = pymysql.connect(host=mysql_hostname, database=mysql_dbname, user=mysql_user, passwd=mysql_password, charset='utf8')
#cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
#cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
#DomainCursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
#GradeCursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
search_log = db_marketanalysis.search_log2


lastDate = ""
#從searchlog前次處理後的位置接續進行處理
lastLogId = getLastlogId()
filter = {}
if lastLogId != "":
	filter = {'_id': {'$gt': lastLogId}}
searchlog_data = search_log.find(filter).sort('since', 1)
for _searchlog in searchlog_data:
	userid = _searchlog['user_id']
	session_id = _searchlog['session_id']
	created_at = _searchlog['since']
	date = created_at.strftime('%Y-%m-%d')
	#print(str(userid) + " - " + session_id + "   " + date)
	
	if (date != lastDate) :
		os.system("python construct_intermedia_collection.py " + date + " s")
		os.system("python city_based_traffic_session.py " + date)
		os.system("python city_based_traffic_usage.py " + date)
		lastDate = date
		print(date)
	else :
		continue

	db_marketanalysis.lastlog.update_one({'type':'visitanalysis_lastId'},{"$set":{'id':_searchlog['_id'],'type':'visitanalysis_lastId'}},upsert=True)
	
client.close()

