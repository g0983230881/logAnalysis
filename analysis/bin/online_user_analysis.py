#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：online_user_analysis.py

import pymongo
from os.path import dirname
import datetime, sys

sys.path.append("..")
from settings import *

client = pymongo.MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
col_onlineUser = db_edumarket["onlinedata"]
col_onMinute_insert = db_edumarket["onlinedata_minute"]


db_marketanalysis = client[mongo_dbname_marketanalysis]
col_lastLog = db_marketanalysis["lastlog"]


# ## edumarket 每秒鐘上線人數 轉為 每分鐘上線人數
server_li = col_onlineUser.distinct("server")
for server_name in server_li:
	lastId_typeName = "onlinedataMinute_lastId_" + server_name
	is_lastlog_exists = col_lastLog.find({"type": lastId_typeName}).explain()['executionStats']['nReturned']  ## 是否存在lastId
	print('col_lastLog.find 1 finished.')

	if (is_lastlog_exists > 0):
		lastId_datas = col_lastLog.find({"type": lastId_typeName})
		for lastId_data in lastId_datas:
			last_ObjectId = lastId_data['id']

		lastProcessed_datas = col_onlineUser.find({"_id": last_ObjectId})
		for lastProcessed_data in lastProcessed_datas:
			start_dt = lastProcessed_data['time']	

		et_datas = col_onlineUser.find({"server": server_name}).sort("time", -1).limit(1)
		for et_data in et_datas:
			tmp_end_dt = et_data["time"]
		end_dt = tmp_end_dt.replace(second=0, microsecond=0)  ## 資料表最後時間 完整單位分鐘
		minusSec_end_dt = end_dt - datetime.timedelta(0, 1)  ## 完整單位分鐘 - 1秒, 為了gte lte可取到正確資料

		lastLog_datas = col_onlineUser.find({"$and": [{"server": server_name}, {"time":{'$gte':minusSec_end_dt, '$lte':end_dt}}]})
		for lastLog_data in lastLog_datas:
			lastLog_id = lastLog_data['_id']
		
		print('col_onlineUser.find 1 finished.')

		while end_dt > start_dt:
			this_start_dt = start_dt.replace(second=0, microsecond=0)
			this_end_dt = this_start_dt + datetime.timedelta(0, 60)
			start_dt += datetime.timedelta(0, 60)  ## 一次+1分鐘
			
			emin_online_count = 0
			emin_mongo_count = 0
			emin_mysql_count = 0
			data_count = col_onlineUser.count_documents({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			# print(data_count, type(data_count))
			mins_results = col_onlineUser.find({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			for mins_result in mins_results:
				emin_online_count += mins_result["online_user"]
				emin_mongo_count += mins_result["mongodb_connection"]
				emin_mysql_count += mins_result["mysql_connection"]

			try:
				emin_online_average = emin_online_count/data_count
			except:
				emin_online_average = 0
			try:
				emin_mongo_average = emin_mongo_count/data_count
			except:
				emin_mongo_average = 0
			try:
				emin_mysql_average = emin_mysql_count/data_count
			except:
				emin_mysql_average = 0

			print(server_name)
			print(this_start_dt)
			print(emin_online_average)
			print('-------------------')

			online_minute_dict = { "server": server_name, "time": this_start_dt.replace(second=0, microsecond=0), "online_user": emin_online_average, "mongodb_connection": emin_mongo_average, "mysql_connection": emin_mysql_average }
			col_onMinute_insert.update_one({"$and": [{"server": server_name}, {"time": this_start_dt.replace(second=0, microsecond=0)}]}, {"$set": online_minute_dict}, upsert=True)			
		col_lastLog.update_one({"type": "onlinedataMinute_lastId_"+server_name}, {"$set":{"id": lastLog_id}}, upsert=True)
		

	else:
		st_datas = col_onlineUser.find({"server": server_name}).sort("time", 1).limit(1)
		print('st_datas 1 finished.')
		for st_data in st_datas:
			start_dt = st_data["time"]

		et_datas = col_onlineUser.find({"server": server_name}).sort("time", -1).limit(1)
		print('et_datas 1 finished.')
		for et_data in et_datas:
			tmp_end_dt = et_data["time"]
		end_dt = tmp_end_dt.replace(second=0, microsecond=0)  ## 資料表最後時間 完整單位分鐘
		minusSec_end_dt = end_dt - datetime.timedelta(0, 1)  ## 完整單位分鐘 - 1秒, 為了gte lte可取到正確資料

		lastLog_datas = col_onlineUser.find({"$and": [{"server": server_name}, {"time":{'$gte':minusSec_end_dt, '$lte':end_dt}}]})
		for lastLog_data in lastLog_datas:
			lastLog_id = lastLog_data['_id']
		
		print('col_onlineUser.find 2 finished.')

		while end_dt > start_dt:
			this_start_dt = start_dt.replace(second=0, microsecond=0)
			this_end_dt = this_start_dt + datetime.timedelta(0, 60)
			start_dt += datetime.timedelta(0, 60)  ## 一次+1分鐘
			
			emin_online_count = 0
			emin_mongo_count = 0
			emin_mysql_count = 0
			data_count = col_onlineUser.count_documents({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			# print(data_count, type(data_count))
			mins_results = col_onlineUser.find({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			for mins_result in mins_results:
				emin_online_count += mins_result["online_user"]
				emin_mongo_count += mins_result["mongodb_connection"]
				emin_mysql_count += mins_result["mysql_connection"]

			try:
				emin_online_average = emin_online_count/data_count
			except:
				emin_online_average = 0
			try:
				emin_mongo_average = emin_mongo_count/data_count
			except:
				emin_mongo_average = 0
			try:
				emin_mysql_average = emin_mysql_count/data_count
			except:
				emin_mysql_average = 0

			print(server_name)
			print(this_start_dt)
			print(emin_online_average)
			print('-------------------')

			online_minute_dict = { "server": server_name, "time": this_start_dt.replace(second=0, microsecond=0), "online_user": emin_online_average, "mongodb_connection": emin_mongo_average, "mysql_connection": emin_mysql_average }
			col_onMinute_insert.update_one({"$and": [{"server": server_name}, {"time": this_start_dt.replace(second=0, microsecond=0)}]}, {"$set": online_minute_dict}, upsert=True)	
		col_lastLog.update_one({"type": "onlinedataMinute_lastId_"+server_name}, {"$set":{"id": lastLog_id}}, upsert=True)



## 每分鐘上線人數 轉為 每小時上線人數
col_onHour_insert = db_edumarket["onlinedata_hour"]
col_onMinute = db_edumarket["onlinedata_minute"]

server_li = col_onMinute.distinct("server")
for server_name in server_li:
	lastId_typeName = "onlineDataHour_lastId_" + server_name
	is_lastlog_exists = col_lastLog.find({"type": lastId_typeName}).explain()['executionStats']['nReturned']  ## 是否存在lastId

	if (is_lastlog_exists > 0):
		lastId_datas = col_lastLog.find({"type": lastId_typeName})
		for lastId_data in lastId_datas:
			last_ObjectId = lastId_data['id']

		lastProcessed_datas = col_onlineUser.find({"_id": last_ObjectId})
		for lastProcessed_data in lastProcessed_datas:
			start_dt = lastProcessed_data['time']	

		et_datas = col_onMinute.find({"server": server_name}).sort("time", -1).limit(1)
		for et_data in et_datas:
			tmp_end_dt = et_data["time"]
		end_dt = tmp_end_dt.replace(minute=0, second=0, microsecond=0)  ## 資料表最後時間 完整單位分鐘
		minusSec_end_dt = end_dt - datetime.timedelta(0, 1)  ## 完整單位小時 - 1秒, 為了gte lte可取到正確資料

		lastLog_datas = col_onlineUser.find({"$and": [{"server": server_name}, {"time":{'$gte':minusSec_end_dt, '$lte':end_dt}}]})
		for lastLog_data in lastLog_datas:
			lastLog_id = lastLog_data['_id']
		print('col_onlineUser.find 3 finished.')

		while end_dt > start_dt:
			this_start_dt = start_dt.replace(minute=0, second=0, microsecond=0)
			this_end_dt = this_start_dt + datetime.timedelta(hours=1)
			start_dt += datetime.timedelta(hours=1)  ## 一次+1分鐘

			ehour_online_count = 0
			ehour_mongo_count = 0
			ehour_mysql_count = 0
			data_count = col_onMinute.count_documents({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			# print(data_count, type(data_count))
			hours_results = col_onMinute.find({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			for hours_result in hours_results:
				ehour_online_count += hours_result["online_user"]
				ehour_mongo_count += hours_result["mongodb_connection"]
				ehour_mysql_count += hours_result["mysql_connection"]

			try:
				ehour_online_average = ehour_online_count/data_count
			except:
				ehour_online_average = 0
			try:
				ehour_mongo_average = ehour_mongo_count/data_count
			except:
				ehour_mongo_average = 0
			try:
				ehour_mysql_average = ehour_mysql_count/data_count
			except:
				ehour_mysql_average = 0

			print(server_name)
			print(this_start_dt)
			print(ehour_online_average)
			print('-------------------')

			online_hour_dict = { "server": server_name, "time": this_start_dt, "online_user": ehour_online_average, "mongodb_connection": ehour_mongo_average, "mysql_connection": ehour_mysql_average }
			col_onHour_insert.update_one({"$and": [{"server": server_name}, {"time": this_start_dt}]}, {"$set": online_hour_dict}, upsert=True)	

		col_lastLog.update_one({"type": "onlineDataHour_lastId_"+server_name}, {"$set":{"id": lastLog_id}}, upsert=True)

	else:
		st_datas = col_onMinute.find({"server": server_name}).sort("time", 1).limit(1)
		for st_data in st_datas:
			start_dt = st_data["time"]

		et_datas = col_onMinute.find({"server": server_name}).sort("time", -1).limit(1)
		for et_data in et_datas:
			tmp_end_dt = et_data["time"]
		end_dt = tmp_end_dt.replace(minute=0, second=0, microsecond=0)  ## 資料表最後時間 完整單位分鐘
		minusSec_end_dt = end_dt - datetime.timedelta(0, 1)  ## 完整單位小時 - 1秒, 為了gte lte可取到正確資料

		lastLog_datas = col_onlineUser.find({"$and": [{"server": server_name}, {"time":{'$gte':minusSec_end_dt, '$lte':end_dt}}]})
		for lastLog_data in lastLog_datas:
			lastLog_id = lastLog_data['_id']
		print('col_onlineUser.find 4 finished.')

		while end_dt > start_dt:
			this_start_dt = start_dt.replace(minute=0, second=0, microsecond=0)
			this_end_dt = this_start_dt + datetime.timedelta(hours=1)
			start_dt += datetime.timedelta(hours=1)  ## 一次+1分鐘

			ehour_online_count = 0
			ehour_mongo_count = 0
			ehour_mysql_count = 0
			data_count = col_onMinute.count_documents({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			# print(data_count, type(data_count))
			hours_results = col_onMinute.find({"$and": [{"server": server_name}, {"time":{'$gte':this_start_dt, '$lte':this_end_dt}}]})
			for hours_result in hours_results:
				ehour_online_count += hours_result["online_user"]
				ehour_mongo_count += hours_result["mongodb_connection"]
				ehour_mysql_count += hours_result["mysql_connection"]

			try:
				ehour_online_average = ehour_online_count/data_count
			except:
				ehour_online_average = 0
			try:
				ehour_mongo_average = ehour_mongo_count/data_count
			except:
				ehour_mongo_average = 0
			try:
				ehour_mysql_average = ehour_mysql_count/data_count
			except:
				ehour_mysql_average = 0

			print(server_name)
			print(this_start_dt)
			print(ehour_online_average)
			print('-------------------')

			online_hour_dict = { "server": server_name, "time": this_start_dt, "online_user": ehour_online_average, "mongodb_connection": ehour_mongo_average, "mysql_connection": ehour_mysql_average }
			col_onHour_insert.update_one({"$and": [{"server": server_name}, {"time": this_start_dt}]}, {"$set": online_hour_dict}, upsert=True)	

		col_lastLog.update_one({"type": "onlineDataHour_lastId_"+server_name}, {"$set":{"id": lastLog_id}}, upsert=True)

print('online_user_analysis finished.')
client.close()

