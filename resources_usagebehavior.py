#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：resource_usage.py
#功能說明：匯整教學資源使用量
#輸入參數：無
#資料來源：marketanalysis -> search_log2
#輸出結果：marketanalysis -> resource_usage_city_month及resource_usage_all
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime
import sys
import pymysql

sys.path.append("..")
from settings import *


conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]

now = datetime.datetime.now()
SQL="select * from member where id={0}"
BehaviorType = ['F', 'M_F', 'L', 'M_L', 'R', 'M_R', 'C', 'M_C', 'MC', 'M_MC', 'SE', 'M_SE', 'SG', 'M_SG', 'SF', 'M_SF', 'ST', 'M_ST', 'SL', 'M_SL', 'H', 'M_H', 'MYRES_ARLink', 'MYRES_ARLink', 'EPedia_L', 'EVideo_L', 'Relation']
firstRecordDate = list(db_marketanalysis.search_log2.aggregate([{'$group':{'_id':'', 'last':{'$min':"$since"}}}]))[0]['last']
year_month_start = firstRecordDate.strftime("%Y-%m")
while now.strftime("%Y-%m") > year_month_start :
	begin_date = datetime.datetime.strptime(year_month_start+'-01', "%Y-%m-%d")
	end_date = begin_date + relativedelta(months=1)

	classCodeCount = {}		
	cityClassCodeCount = {}
	cityCount = {}
	usageCount = 0
	class_code = ''
	main_col = ''
	created_at = ''
	from_ip = ''
	member_id = 0
	city_id = 0
	userBehaviorLog = db_marketanalysis.search_log2.find({"since": { "$gte": begin_date, "$lt": end_date}})#.sort([('from_ip',1),('class_code',1), ('since',1)])

	userBehaviorLogStart = datetime.datetime.now()
	for userlog in userBehaviorLog :
		#同一分鐘內的相同行為不予列計
		#if (from_ip!=userlog['from_ip'] and class_code!=userlog['class_code'] and main_col!=userlog['main_col'] and created_at!=unicode(userlog['since'])[0:16]) :
			member_id = userlog['user_id']
			session_id = userlog['session_id']
			from_ip = userlog['from_ip']
			class_code = userlog['class_code']
			#main_col = userlog['main_col']
			created_at = str(userlog['since'])[0:16]

			identifyMember = db_marketanalysis.identify_member.find({'session_id':session_id})
			for _identifyMember in identifyMember :
				if member_id <= 0 and  _identifyMember['user_id'] > 0 :
					member_id = _identifyMember['user_id']

			if member_id > 0 :
				cursor1.execute(SQL.format(member_id))
				memberRow = cursor1.fetchone()
				if memberRow is not None:
					city_id = memberRow.get('city_id')
					if city_id > 0 and class_code in BehaviorType :
						if set([str(city_id)]).issubset(cityCount) == True :
							cityCount[str(city_id)] = cityCount.get(str(city_id)) + 1
						else :
							cityCount[str(city_id)] = 1

						cityidClassCode = str(city_id) + '_' + class_code
						if set([cityidClassCode]).issubset(cityClassCodeCount) == True :
							cityClassCodeCount[cityidClassCode] = cityClassCodeCount.get(cityidClassCode) + 1	
						else :
							cityClassCodeCount[cityidClassCode] = 1

			if class_code in BehaviorType :
				if set([class_code]).issubset(classCodeCount) == True :
					classCodeCount[class_code] = classCodeCount.get(class_code) + 1
				else :
					classCodeCount[class_code] = 1
				usageCount = usageCount + 1

		#else :
			#continue
	userBehaviorLogEnd = datetime.datetime.now()
	print('userBehaviorLog_loopDeltaTime:	{}.'.format(userBehaviorLogEnd - userBehaviorLogStart))

	cityCountStart = datetime.datetime.now()
	for city_id, count in cityCount.items() :
		usageClassCodeCount = {}
		for key,value in cityClassCodeCount.items() :
			#print(city_id + ' ' + key + ' ' + str(value))
			if key.startswith(str(city_id) + '_') == True :
				usageClassCodeCount[key.split('_')[1]] = value

		db_marketanalysis.resource_usagebehavior_city_month.update_one(
			{'year_month': end_date.strftime("%Y-%m"), 'city_id':int(city_id) },{"$set":
			{'year_month': end_date.strftime("%Y-%m"), 'city_id':int(city_id), 'total':count, 'usage':usageClassCodeCount, 'updated_at' : datetime.datetime.now() }},
			upsert=True
		)
	cityCountEnd = datetime.datetime.now()
	print('cityCount_loopDeltaTime:	{}.'.format(cityCountEnd - cityCountStart))


	db_marketanalysis.resource_usagebehavior_all.update_one(
		{'year_month': end_date.strftime("%Y-%m") },{"$set":
		{'year_month': end_date.strftime("%Y-%m"), 'total' : usageCount, 'usage' : classCodeCount, 'updated_at' : datetime.datetime.now() }},
		upsert=True
	)
	year_month_start = end_date.strftime("%Y-%m")
