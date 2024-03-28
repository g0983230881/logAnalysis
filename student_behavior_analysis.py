#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_behavior_analysis.py
#功能說明：依照不同屬性，進行學習行為分析
#輸入參數：無
#資料來源：edumarket -> searchlog
#輸出結果：marketanalysis -> member_behavior_count

from pymongo import MongoClient
from os.path import dirname
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import sys
import pymysql

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor3 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor4 = conn.cursor(cursor=pymysql.cursors.DictCursor)

res_objSQL = 'select * from resources_objfile where isdelete="N" and objectfile_id={0}'
resources_kmSQL = 'select * from resources_km where isdelete="N" and type="level" and resources_id=%s'
level2SQL = 'select * from KM_Level2 where isdelete="N" and id=%s'


lastLog_id = ''
ini_start_dt = ''
ini_end_dt = ''

is_lastlog_exists = db_marketanalysis.lastlog.find({'type': 'student_behavior_analysis'}).explain()['executionStats']['nReturned']  ## 是否存在lastId
if is_lastlog_exists == 1:
    lastLog_data = db_marketanalysis.lastlog.find({'type': 'student_behavior_analysis'})
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
    yearMonth_data = db_edumarket.searchlog.find().sort('since', 1).limit(1);    
    for yearMonth in yearMonth_data:
        ini_start_dt = yearMonth['since']
    end_date_data = db_edumarket.searchlog.find().sort('since', -1).limit(1);
    for end_date in end_date_data:
        lastLog_id = end_date['_id']
        ini_end_dt = end_date['since']

start_year = ini_start_dt.year
start_month = ini_start_dt.month
end_year = ini_end_dt.year
end_month = ini_end_dt.month

start_dt = datetime.strptime(str(start_year) + '-' + str(start_month), '%Y-%m')
end_dt = datetime.strptime(str(end_year) + '-' + str(end_month), '%Y-%m') + relativedelta(months=+1)  


## 找出各地理區域的使用者編號
north_list = [1, 2, 3, 4, 22, 17, 18]
center_list = [5, 6, 7, 8, 9, 19]
south_list = [10, 11, 12, 13, 20, 21, 23]
east_list = [14, 15]
out_list = [16, 24, 25]

north_student_list = []	   
SQL3 = 'select * from member where status="N" and roletype="03" and city_id in('
for north_city in north_list:
	SQL3 += str(north_city)
	if (north_list[-1] != north_city):
		SQL3 += ','
	else:
		SQL3 += ')'
cursor.execute(SQL3)
north_member_datas = cursor.fetchall()
for north_member_data in north_member_datas:
	north_student_list.append(north_member_data.get('id')) 

center_student_list = []	   
SQL3 = 'select * from member where status="N" and roletype="03" and city_id in('
for center_city in center_list:
	SQL3 += str(center_city)
	if (center_list[-1] != center_city):
		SQL3 += ','
	else:
		SQL3 += ')'
cursor.execute(SQL3)
center_member_datas = cursor.fetchall()
for center_member_data in center_member_datas:
	center_student_list.append(center_member_data.get('id'))

south_student_list = []	   
SQL3 = 'select * from member where status="N" and roletype="03" and city_id in('
for south_city in south_list:
	SQL3 += str(south_city)
	if (south_list[-1] != south_city):
		SQL3 += ','
	else:
		SQL3 += ')'
cursor.execute(SQL3)
south_member_datas = cursor.fetchall()
for south_member_data in south_member_datas:
	south_student_list.append(south_member_data.get('id'))

east_student_list = []	   
SQL3 = 'select * from member where status="N" and roletype="03" and city_id in('
for east_city in east_list:
	SQL3 += str(east_city)
	if (east_list[-1] != east_city):
		SQL3 += ','
	else:
		SQL3 += ')'
cursor.execute(SQL3)
east_member_datas = cursor.fetchall()
for east_member_data in east_member_datas:
	east_student_list.append(east_member_data.get('id'))

out_student_list = []	   
SQL3 = 'select * from member where status="N" and roletype="03" and city_id in('
for out_city in out_list:
	SQL3 += str(out_city)
	if (out_list[-1] != out_city):
		SQL3 += ','
	else:
		SQL3 += ')'
cursor.execute(SQL3)
out_member_datas = cursor.fetchall()
for out_member_data in out_member_datas:
	out_student_list.append(out_member_data.get('id'))


## 找出學生的編號
student_list = []
SQL = 'select * from member where roletype="03" and status="N"'
cursor.execute(SQL)
member_datas = cursor.fetchall()
for member_data in member_datas:
	student_list.append(member_data.get('id'))

while start_dt < end_dt:
	this_startDt = start_dt + timedelta(hours=-8)
	this_endDt = start_dt + relativedelta(months=+1) + timedelta(hours=-8)
	right_endDt = start_dt + relativedelta(months=+1)
	print(str(this_startDt) + ' - ' + str(this_endDt))
	start_date = str(this_startDt.year) + '-' + str(this_startDt.month);

	count_level1 = 0
	count_level2 = 0
	count_level3 = 0
	count_level4 = 0
	count_level5 = 0
	this_month_student_list = []  ## 該月份登入使用資源的學生
	this_north_student_list = []  ## 該月份北區 登入使用資源的學生
	this_center_student_list = []  ## 該月份中區 登入使用資源的學生
	this_south_student_list = []  ## 該月份南區 登入使用資源的學生
	this_east_student_list = []  ## 該月份東區 登入使用資源的學生
	this_out_student_list = []  ## 該月份外島 登入使用資源的學生

	user_list = db_edumarket.searchlog.distinct('user_id', {'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
	for thismt_user in user_list:
		if thismt_user in student_list:
			this_month_student_list.append(thismt_user)
		if thismt_user in north_student_list:
			this_north_student_list.append(thismt_user)
		if thismt_user in center_student_list:
			this_center_student_list.append(thismt_user)
		if thismt_user in south_student_list:
			this_south_student_list.append(thismt_user)
		if thismt_user in east_student_list:
			this_east_student_list.append(thismt_user)
		if thismt_user in out_student_list:
			this_out_student_list.append(thismt_user)

	north_count = 0
	center_count = 0
	south_count = 0
	east_count = 0
	out_count = 0
	for this_north_student in this_north_student_list:
		if db_edumarket.searchlog.count_documents({'user_id': this_north_student, 'class_code': {'$in': ['R', 'F']}}) > 0:
			north_count += 1

	for this_center_student in this_center_student_list:
		if db_edumarket.searchlog.count_documents({'user_id': this_center_student, 'class_code': {'$in': ['R', 'F']}}) > 0:
			center_count += 1

	for this_south_student in this_south_student_list:
		if db_edumarket.searchlog.count_documents({'user_id': this_south_student, 'class_code': {'$in': ['R', 'F']}}) > 0:
			south_count += 1

	for this_east_student in this_east_student_list:
		if db_edumarket.searchlog.count_documents({'user_id': this_east_student, 'class_code': {'$in': ['R', 'F']}}) > 0:
			east_count +=1

	for this_out_student in this_out_student_list:
		if db_edumarket.searchlog.count_documents({'user_id': this_out_student, 'class_code': {'$in': ['R', 'F']}}) > 0:
			out_count +=1
	
	db_marketanalysis.student_usage_region.update_many(
		{'region' : '地理區域(北)', 'year_month': start_date},
		{'$set' : {'region' : '地理區域(北)', 'year_month': start_date, 'region_count': north_count}},
		upsert=True
	)
	db_marketanalysis.student_usage_region.update_many(
		{'region' : '地理區域(中)', 'year_month': start_date},
		{'$set' : {'region' : '地理區域(中)', 'year_month': start_date, 'region_count': center_count}},
		upsert=True
	)
	db_marketanalysis.student_usage_region.update_many(
		{'region' : '地理區域(南)', 'year_month': start_date},
		{'$set' : {'region' : '地理區域(南)', 'year_month': start_date, 'region_count': south_count}},
		upsert=True
	)
	db_marketanalysis.student_usage_region.update_many(
		{'region' : '地理區域(東)', 'year_month': start_date},
		{'$set' : {'region' : '地理區域(東)', 'year_month': start_date, 'region_count': east_count}},
		upsert=True
	)
	db_marketanalysis.student_usage_region.update_many(
		{'region' : '地理區域(離島)', 'year_month': start_date},
		{'$set' : {'region' : '地理區域(離島)', 'year_month': start_date, 'region_count': out_count}},
		upsert=True
	)


	## 月份學生使用各學習階段	
	for this_month_student in this_month_student_list:
		level2_id_list = []  ## 存放km_level2 (學習階段)編號
		level2_name_list = []  ## 存放km_level2 中文名稱

		log_datas = db_edumarket.searchlog.find({'user_id': this_month_student, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
		for log_data in log_datas:
			class_code = log_data.get("class_code")
			res_id = log_data.get("main_col")

			## 下載的資源編號不同, 需從resources_objfile找回資源編號
			if (res_id != '' and class_code == 'F'):
				cursor2.execute(res_objSQL.format(res_id))
				res_objRow = cursor2.fetchone()
				if res_objRow is not None:	
					res_id = str(res_objRow.get('resources_id'))

			if (res_id != '' and res_id.isdigit()):
				cursor3.execute(resources_kmSQL, int(res_id))  ## resources_km 查詢學習階段
				res_km_datas = cursor3.fetchall()
				for res_km_data in res_km_datas:
					km_content = res_km_data.get('content')
					content_list = str(km_content).split(',')

					if (len(content_list) > 1):
						level2_id_list.append(content_list[1].replace("'", ""))

		level2_id_list = list(dict.fromkeys(level2_id_list))
		if (len(level2_id_list) > 0):
			# print('student_id: ' + str(this_month_student))
			for level2_id in level2_id_list:
				cursor4.execute(level2SQL, int(level2_id))
				level2_datas = cursor4.fetchall()
				for level2_data in level2_datas:
					level2_name_list.append(level2_data.get('name'))

			level2_name_list = list(dict.fromkeys(level2_name_list))
			for level2_name in level2_name_list:
				if (level2_name == '一'):
					count_level1 += 1
				if (level2_name == '二'):
					count_level2 += 1
				if (level2_name == '三'):
					count_level3 += 1
				if (level2_name == '四'):
					count_level4 += 1
				if (level2_name == '五'):
					count_level5 += 1
				
	db_marketanalysis.student_usage_level.update_many(
		{'level' : '一', 'year_month': start_date},
		{'$set' : {'level' : '一', 'year_month': start_date, 'resources_count': count_level1}},
		upsert=True
	)
	db_marketanalysis.student_usage_level.update_many(
		{'level' : '二', 'year_month': start_date},
		{'$set' : {'level' : '二', 'year_month': start_date, 'resources_count': count_level2}},
		upsert=True
	)
	db_marketanalysis.student_usage_level.update_many(
		{'level' : '三', 'year_month': start_date},
		{'$set' : {'level' : '三', 'year_month': start_date, 'resources_count': count_level3}},
		upsert=True
	)
	db_marketanalysis.student_usage_level.update_many(
		{'level' : '四', 'year_month': start_date},
		{'$set' : {'level' : '四', 'year_month': start_date, 'resources_count': count_level4}},
		upsert=True
	)
	db_marketanalysis.student_usage_level.update_many(
		{'level' : '五', 'year_month': start_date},
		{'$set' : {'level' : '五', 'year_month': start_date, 'resources_count': count_level5}},
		upsert=True
	)

	start_dt = right_endDt 

start_dt = datetime.strptime(str(start_year) + '-' + str(start_month), '%Y-%m')
end_dt = datetime.strptime(str(end_year) + '-' + str(end_month), '%Y-%m') + relativedelta(months=+1) 


clock_02_count = 0
clock_04_count = 0
clock_06_count = 0
clock_08_count = 0
clock_10_count = 0
clock_12_count = 0
clock_14_count = 0
clock_16_count = 0
clock_18_count = 0
clock_20_count = 0
clock_22_count = 0
clock_24_count = 0
hour_month = start_dt.month 

while start_dt < end_dt:
	this_startDt = start_dt
	this_endDt = start_dt + relativedelta(days=+1)

	this_day_student_list = []
	user_list = db_edumarket.searchlog.distinct('user_id', {'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
	for thisday_user in user_list:
		if thisday_user in student_list:
			this_day_student_list.append(thisday_user)

	if this_day_student_list != []:
		for student in this_day_student_list:
			this_start_hour = this_startDt + timedelta(hours=-8) 
			this_end_hour = this_startDt + timedelta(hours=2) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_02_count += 1

			this_start_hour = this_startDt + timedelta(hours=2) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=4) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_04_count += 1

			this_start_hour = this_startDt + timedelta(hours=4) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=6) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_06_count += 1

			this_start_hour = this_startDt + timedelta(hours=6) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=8) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_08_count += 1

			this_start_hour = this_startDt + timedelta(hours=8) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=10) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_10_count += 1

			this_start_hour = this_startDt + timedelta(hours=10) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=12) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_12_count += 1

			this_start_hour = this_startDt + timedelta(hours=12) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=14) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_14_count += 1

			this_start_hour = this_startDt + timedelta(hours=14) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=16) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_16_count += 1

			this_start_hour = this_startDt + timedelta(hours=16) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=18) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_18_count += 1

			this_start_hour = this_startDt + timedelta(hours=18) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=20) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_20_count += 1

			this_start_hour = this_startDt + timedelta(hours=20) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=22) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_22_count += 1

			this_start_hour = this_startDt + timedelta(hours=22) + timedelta(hours=-8)
			this_end_hour = this_startDt + timedelta(hours=24) + timedelta(hours=-8)
			if db_edumarket.searchlog.count_documents({'user_id': student, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
				clock_24_count += 1
	# print(this_startDt)
	# print(clock_02_count)
	# print(clock_04_count)

	if (hour_month != this_startDt.month):
		print('upserting....')
		start_date = str((this_startDt + relativedelta(months = -1)).year) + '-' + str((this_startDt + relativedelta(months = -1)).month);

		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '0-2時', 'year_month': start_date},
			{'$set' : {'period' : '0-2時', 'behavior_count': clock_02_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '2-4時', 'year_month': start_date},
			{'$set' : {'period' : '2-4時', 'behavior_count': clock_04_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '4-6時', 'year_month': start_date},
			{'$set' : {'period' : '4-6時', 'behavior_count': clock_06_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '6-8時', 'year_month': start_date},
			{'$set' : {'period' : '6-8時', 'behavior_count': clock_08_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '8-10時', 'year_month': start_date},
			{'$set' : {'period' : '8-10時', 'behavior_count': clock_10_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '10-12時', 'year_month': start_date},
			{'$set' : {'period' : '10-12時', 'behavior_count': clock_12_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '12-14時', 'year_month': start_date},
			{'$set' : {'period' : '12-14時', 'behavior_count': clock_14_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '14-16時', 'year_month': start_date},
			{'$set' : {'period' : '14-16時', 'behavior_count': clock_16_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '16-18時', 'year_month': start_date},
			{'$set' : {'period' : '16-18時', 'behavior_count': clock_18_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '18-20時', 'year_month': start_date},
			{'$set' : {'period' : '18-20時', 'behavior_count': clock_20_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '20-22時', 'year_month': start_date},
			{'$set' : {'period' : '20-22時', 'behavior_count': clock_22_count, 'year_month': start_date}},
			upsert=True
		)
		db_marketanalysis.student_usage_dayperiod.update_many(
			{'period' : '22-24時', 'year_month': start_date},
			{'$set' : {'period' : '22-24時', 'behavior_count': clock_24_count, 'year_month': start_date}},
			upsert=True
		)

		hour_month = this_startDt.month

		clock_02_count = 0
		clock_04_count = 0
		clock_06_count = 0
		clock_08_count = 0
		clock_10_count = 0
		clock_12_count = 0
		clock_14_count = 0
		clock_16_count = 0
		clock_18_count = 0
		clock_20_count = 0
		clock_22_count = 0
		clock_24_count = 0

	start_dt = this_endDt

db_marketanalysis.lastlog.update_many({'type': 'student_behavior_analysis'}, {'$set': {'id': lastLog_id}}, upsert=True)	

cursor.close()
client.close()