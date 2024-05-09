#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：city_student_learning_analysis.py
#功能說明：依各縣市進行每月學生學習行為分析
#輸入參數：無
#資料來源：marketanalysis -> search_log2
#輸出結果：marketanalysis -> city_student_learning_sgender	性別
#						 -> city_student_learning_level		學習階段
#						 -> city_student_learning_dayperiod	學習時段
#						 -> city_student_learning_school	學校

from pymongo import MongoClient
from os.path import dirname
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from operator import itemgetter
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

is_lastlog_exists = db_marketanalysis.lastlog.find({'type': 'city_student_learning_analysis'}).explain()['executionStats']['nReturned']  ## 是否存在lastId
if is_lastlog_exists == 1:
	lastLog_data = db_marketanalysis.lastlog.find({'type': 'city_student_learning_analysis'})
	for lastLog in lastLog_data:
		lastLog_id = lastLog['id']

	is_last_datas_exists = db_marketanalysis.search_log2.find({'_id': lastLog_id}).explain()['executionStats']['nReturned']
	if is_last_datas_exists == 1:
		last_datas = db_marketanalysis.search_log2.find({'_id': lastLog_id})
		for last_data in last_datas:
			ini_start_dt = last_data['since']
	else:
		yearMonth_data = db_marketanalysis.search_log2.find().sort('since', 1).limit(1);	
		for yearMonth in yearMonth_data:
			ini_start_dt = yearMonth['since']

	is_end_date_data_exists = db_marketanalysis.search_log2.find().sort('since', -1).limit(1).explain()['executionStats']['nReturned']
	if is_end_date_data_exists == 1:
		end_date_data = db_marketanalysis.search_log2.find().sort('since', -1).limit(1);
		for end_date in end_date_data:
			lastLog_id = end_date['_id']
			ini_end_dt = end_date['since']
	else:
		end_date_data = db_marketanalysis.search_log2.find().sort('since', -1).limit(1);
		for end_date in end_date_data:
			lastLog_id = end_date['_id']
			ini_end_dt = end_date['since']
else:
	## 找出search_log2中開始與結束時間
	yearMonth_data = db_marketanalysis.search_log2.find().sort('since', 1).limit(1);	
	for yearMonth in yearMonth_data:
		ini_start_dt = yearMonth['since']
	end_date_data = db_marketanalysis.search_log2.find().sort('since', -1).limit(1);
	for end_date in end_date_data:
		lastLog_id = end_date['_id']
		ini_end_dt = end_date['since']

start_year = ini_start_dt.year
start_month = ini_start_dt.month
end_year = ini_end_dt.year
end_month = ini_end_dt.month

start_dt = datetime.strptime(str(start_year) + '-' + str(start_month), '%Y-%m')
end_dt = datetime.strptime(str(end_year) + '-' + str(end_month), '%Y-%m') + relativedelta(months=+1)  

##找出所有縣市，跳過台中縣6、台南縣11、高雄縣12
city_list = []
citySQL = 'select id from city where isdelete="N"'
cursor.execute(citySQL)
city_datas = cursor.fetchall()
for city_data in city_datas:
	if city_data.get('id') != 6 or city_data.get('id') != 11 or city_data.get('id') != 12:
		city_list.append(city_data.get('id'))
		
## 找出所有學校
schoolname_list = {}
schoolSQL = 'select sno, school from school where isdelete="N"'
cursor.execute(schoolSQL)
school_datas = cursor.fetchall()
for school_data in school_datas:
	schoolname_list[school_data.get('sno')] = school_data.get('school')

## 找出所有學生
student_list = {}
studentSQL = 'select id, gender, city_id, school_id from member where roletype="03" and status="N"'
cursor.execute(studentSQL)
student_datas = cursor.fetchall()
for student_data in student_datas:
	student_list[student_data.get('id')] = {'gender':student_data.get('gender'),
											'city_id':student_data.get('city_id'),
											'school_id':student_data.get('school_id')}

while start_dt < end_dt:
	this_startDt = start_dt + timedelta(hours=-8)
	this_endDt = start_dt + relativedelta(months=+1) + timedelta(hours=-8)
	right_endDt = start_dt + relativedelta(months=+1)
	print(str(this_startDt) + ' - ' + str(this_endDt))
	yearMonth = str(this_startDt.year) + '-' + str(this_startDt.month);
	
	gender_list = {}
	level_list = {}
	school_list = {}
	for city_no in city_list:
		gender_list[city_no] = {'male':0, 'female':0}
		level_list[city_no] = {'level1':0, 'level2':0,'level3':0,'level4':0,'level5':0}
		school_list[city_no] = {}

	user_list = db_marketanalysis.search_log2.distinct('user_id', {'user_id': {'$gt': 0}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
	for thismt_user in user_list:
		if student_list.get(thismt_user) != None:
			student = student_list.get(thismt_user)
			if student['city_id'] > 0:
				##性別
				if student['gender'] == 'M':
					gender_list[student.get('city_id')]['male'] += 1
				elif student['gender'] == 'F':
					gender_list[student.get('city_id')]['female'] += 1
					
				##學習階段
				level2_id_list = []  ## 存放km_level2 (學習階段)編號
				level2_name_list = []  ## 存放km_level2 中文名稱
				log_datas = db_marketanalysis.search_log2.find({'user_id': thismt_user, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
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
								level2_id_list.append(content_list[1])
								
				level2_id_list = list(dict.fromkeys(level2_id_list))
				if (len(level2_id_list) > 0):
					for level2_id in level2_id_list:
						cursor4.execute(level2SQL, int(level2_id))
						level2_datas = cursor4.fetchall()
						for level2_data in level2_datas:
							level2_name_list.append(level2_data.get('name'))
					level2_name_list = list(dict.fromkeys(level2_name_list))
					
					for level2_name in level2_name_list:
						if (level2_name == '一'):
							level_list[student.get('city_id')]['level1'] += 1
						if (level2_name == '二'):
							level_list[student.get('city_id')]['level2'] += 1
						if (level2_name == '三'):
							level_list[student.get('city_id')]['level3'] += 1
						if (level2_name == '四'):
							level_list[student.get('city_id')]['level4'] += 1
						if (level2_name == '五'):
							level_list[student.get('city_id')]['level5'] += 1
				
				##學校
				if student['school_id'] != None and student['school_id'] != '':
					if school_list[student['city_id']].get(student['school_id']) == None:
						school_list[student['city_id']][schoolname_list[student['school_id']]] = 1		#list裡沒有這所學校就建立一個key
					else:
						school_list[student['city_id']][schoolname_list[student['school_id']]] += 1
						
	for city_id in city_list:
		#更新性別
		male_count = gender_list[city_id]['male']
		female_count = gender_list[city_id]['female']
		db_marketanalysis.city_student_learning_sgender.update_many(
			{'city_id':city_id, 'year_month': yearMonth},
			{'$set' : {'male' : male_count, 'female' : female_count, 'year_month': yearMonth}},
			upsert=True
		)
		
		#更新學習階段
		level1_count = level_list[city_id]['level1']
		level2_count = level_list[city_id]['level2']
		level3_count = level_list[city_id]['level3']
		level4_count = level_list[city_id]['level4']
		level5_count = level_list[city_id]['level5']
		db_marketanalysis.city_student_learning_level.update_many(
			{'city_id':city_id, 'year_month': yearMonth},
			{'$set' : {'level1' : level1_count,
					   'level2' : level2_count, 
					   'level3' : level3_count, 
					   'level4' : level4_count, 
					   'level5' : level5_count, 
					   'year_month': yearMonth}},
			upsert=True
		)
		
		#更新學校
		sort_dict = {}
		if school_list[city_id] != {}:	
			sort_list = list(school_list[city_id].items())
			sort_list.sort(key=itemgetter(1), reverse=True)	#多到少排序
			for i in range(len(sort_list)):
				if i < 10:	#最多取10所學校
					sort_dict[sort_list[i][0]] = sort_list[i][1]
			db_marketanalysis.city_student_learning_school.update_many(
				{'city_id':city_id, 'year_month': yearMonth},
				{'$set' : {'school':sort_dict, 'year_month': yearMonth}},
				upsert=True
			)
			
	start_dt = right_endDt 

start_dt = datetime.strptime(str(start_year) + '-' + str(start_month), '%Y-%m')
end_dt = datetime.strptime(str(end_year) + '-' + str(end_month), '%Y-%m') + relativedelta(months=+1) 


clock_list = {}
for city_no in city_list:
	clock_list[city_no] = {'clock_02':0, 'clock_04':0, 'clock_06':0, 'clock_08':0, 'clock_10':0, 'clock_12':0, 
						   'clock_14':0, 'clock_16':0, 'clock_18':0, 'clock_20':0, 'clock_22':0, 'clock_24':0}

hour_month = start_dt.month 
while start_dt < end_dt:
	this_startDt = start_dt
	this_endDt = start_dt + relativedelta(days=+1)

	user_list = db_marketanalysis.search_log2.distinct('user_id', {'user_id': {'$gt': 0}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
	for thisday_user in user_list:
		if student_list.get(thisday_user) != None:
			student = student_list.get(thisday_user)
			if student['city_id'] > 0:
				for hour_gap in range(0, 24, 2):
					this_start_hour = this_startDt + timedelta(hours=-8) + timedelta(hours=hour_gap)
					this_end_hour = this_startDt + timedelta(hours=-8) + timedelta(hours=hour_gap+2)
					if db_marketanalysis.search_log2.count_documents({'user_id': thisday_user, 'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}}) > 0:
						if hour_gap == 0:
							clock_list[student.get('city_id')]['clock_02'] += 1
						elif hour_gap == 2:
							clock_list[student.get('city_id')]['clock_04'] += 1
						elif hour_gap == 4:
							clock_list[student.get('city_id')]['clock_06'] += 1
						elif hour_gap == 6:
							clock_list[student.get('city_id')]['clock_08'] += 1
						elif hour_gap == 8:
							clock_list[student.get('city_id')]['clock_10'] += 1
						elif hour_gap == 10:
							clock_list[student.get('city_id')]['clock_12'] += 1
						elif hour_gap == 12:
							clock_list[student.get('city_id')]['clock_14'] += 1
						elif hour_gap == 14:
							clock_list[student.get('city_id')]['clock_16'] += 1
						elif hour_gap == 16:
							clock_list[student.get('city_id')]['clock_18'] += 1
						elif hour_gap == 18:
							clock_list[student.get('city_id')]['clock_20'] += 1
						elif hour_gap == 20:
							clock_list[student.get('city_id')]['clock_22'] += 1
						elif hour_gap == 22:
							clock_list[student.get('city_id')]['clock_24'] += 1
	## 更新學習時段
	if (hour_month != this_startDt.month):
		print('upserting....')
		yearMonth = str((this_startDt + relativedelta(months = -1)).year) + '-' + str((this_startDt + relativedelta(months = -1)).month);
		
		for city_id in city_list:
			clock_02_count = clock_list[city_id]['clock_02']
			clock_04_count = clock_list[city_id]['clock_04']
			clock_06_count = clock_list[city_id]['clock_06']
			clock_08_count = clock_list[city_id]['clock_08']
			clock_10_count = clock_list[city_id]['clock_10']
			clock_12_count = clock_list[city_id]['clock_12']
			clock_14_count = clock_list[city_id]['clock_14']
			clock_16_count = clock_list[city_id]['clock_16']
			clock_18_count = clock_list[city_id]['clock_18']
			clock_20_count = clock_list[city_id]['clock_20']
			clock_22_count = clock_list[city_id]['clock_22']
			clock_24_count = clock_list[city_id]['clock_24']
			db_marketanalysis.city_student_learning_dayperiod.update_many(
				{'city_id':city_id, 'year_month': yearMonth},
				{'$set' : {'clock_02_count' : clock_02_count,
						   'clock_04_count' : clock_04_count,
						   'clock_06_count' : clock_06_count,
						   'clock_08_count' : clock_08_count,
						   'clock_10_count' : clock_10_count,
						   'clock_12_count' : clock_12_count,
						   'clock_14_count' : clock_14_count,
						   'clock_16_count' : clock_16_count,
						   'clock_18_count' : clock_18_count,
						   'clock_20_count' : clock_20_count,
						   'clock_22_count' : clock_22_count,
						   'clock_24_count' : clock_24_count,
						   'year_month': yearMonth}},
				upsert=True
			)

		hour_month = this_startDt.month
		for city_no in city_list:
			clock_list[city_no] = {'clock_02':0, 'clock_04':0, 'clock_06':0, 'clock_08':0, 'clock_10':0, 'clock_12':0, 
								   'clock_14':0, 'clock_16':0, 'clock_18':0, 'clock_20':0, 'clock_22':0, 'clock_24':0}
		
	start_dt = this_endDt

db_marketanalysis.lastlog.update_many({'type': 'city_student_learning_analysis'}, {'$set': {'id': lastLog_id}}, upsert=True)	

cursor.close()
client.close()
