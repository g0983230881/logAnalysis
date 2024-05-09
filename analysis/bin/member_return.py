#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_return.py
#功能說明：會員無訪數與新會員數統計
#輸入參數：無
#資料來源：MongoDB:edumarket -> educloud_member及educloud_loginhistory
#輸出結果：MongoDB:marketanalysis -> member_return_all及member_return_city_month
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime
import sys
import pymysql
import pandas as pd

sys.path.append("..")
from settings import *


conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]
educloud_member_coll = db_edumarket.educloud_member
educloud_loginhistory_coll = db_edumarket.educloud_loginhistory

SQL = "select * from member where userid='cloudid://{0}' and created_at < '{1}'"
now = datetime.datetime.now()
firstRecordDate = list(educloud_member_coll.aggregate([{'$group':{'_id':'', 'last':{'$min':"$created_at"}}}]))[0]['last']
year_month_start = firstRecordDate.strftime("%Y-%m")
#計算各月份的新會員數及舊會員的回訪數量
whileStart1 = datetime.datetime.now()
while now.strftime("%Y-%m") >= year_month_start :
	begin_date = datetime.datetime.strptime(year_month_start+'-01', "%Y-%m-%d")
	end_date = begin_date + relativedelta(months=1)
	#依月份統計初次到訪的會員人數
	firstLoginMemberCount_ByMonth = educloud_member_coll.count_documents({"created_at": { "$gte": begin_date, "$lt": end_date}})
	
	#計算每一個會員的回訪次數
	memberReturn = {}
	memberReturnCount = 0
	sameMemberWithTime = ''
	educloud_loginhistory = educloud_loginhistory_coll.aggregate([{"$match" : {"created_at": [{"$gte": begin_date, "$lt": end_date}]}}, {"$sort" : {"UserAccount":1, "created":1}}], allowDiskUse=True)

	educloud_loginhistoryStart = datetime.datetime.now()
	for loginhistory in educloud_loginhistory :
		email = loginhistory['UserAccount']
		#同一天內的登入記錄只算一筆
		#print(sameMemberWithTime + '  <-->   ' + email + loginhistory['created_at'].strftime("%Y%m%d"))
		if sameMemberWithTime != (email + loginhistory['created_at'].strftime("%Y%m%d")) :
			sameMemberWithTime = email + loginhistory['created_at'].strftime("%Y%m%d")
			formatted_SQL = SQL.format(email, loginhistory['created_at'].strftime("%Y-%m-%d %H:%M:%S"))
			memberinfo = pd.read_sql_query(formatted_SQL, conn)
			if set([email]).issubset(memberReturn) == True :
				memberReturn[email] = memberReturn.get(email) + len(memberinfo)
			else :
				memberReturn[email] = len(memberinfo)
		else :
			continue
		#print(len(memberinfo))
	educloud_loginhistoryEnd = datetime.datetime.now()
	print('educloud_loginhistory_loopDeltaTime:	{}.'.format(educloud_loginhistoryEnd - educloud_loginhistoryStart))

	memberReturnStart = datetime.datetime.now()
	for email, returntimes in memberReturn.items():
		if (returntimes > 0) :
			memberReturnCount = memberReturnCount + 1
	memberReturnEnd = datetime.datetime.now()
	print('memberReturn_loopDeltaTime:		{}.'.format(memberReturnEnd - memberReturnStart))


	print(year_month_start + ' ' + str(firstLoginMemberCount_ByMonth) + '  ' + str(memberReturnCount))
	db_marketanalysis.member_return_all.update_one(
		{'year_month' : year_month_start},{"$set":
		{'year_month' : year_month_start, 'first' : firstLoginMemberCount_ByMonth, 'return' : memberReturnCount, 'updated_at' : datetime.datetime.now()}},
		upsert=True
	)
	year_month_start = end_date.strftime("%Y-%m")
whileEnd1 = datetime.datetime.now()
print('while1_loopDeltaTime:			{}.'.format(whileEnd1 - whileStart1))

#依縣市及月份計算新會員數及舊會員的回訪數量
year_month_start = firstRecordDate.strftime("%Y-%m")
cityId = educloud_member_coll.distinct('city_id')

cityIdStart = datetime.datetime.now()
for city_id in cityId:
	#print(city_id)
	year_month_start = firstRecordDate.strftime("%Y-%m")

	whileStart2 = datetime.datetime.now()
	while now.strftime('%Y-%m') >= year_month_start :
		begin_date = datetime.datetime.strptime(year_month_start+'-01', "%Y-%m-%d")
		end_date = begin_date + relativedelta(months=1)

		#while now.strftime('%Y-%m') > year_month_start :
		#依縣市及月份統計初次到訪的會員人數
		firstLoginMemberCount_ByCityMonth = educloud_member_coll.count_documents({'city_id':city_id, "created_at": { "$gte": begin_date, "$lt": end_date}})
		
		#依月份統計初次到訪的會員人數
		firstLoginMemberCount_ByMonth = educloud_member_coll.count_documents({"created_at": { "$gte": begin_date, "$lt": end_date}})

		#計算每一個會員的回訪次數
		cityMemberReturn = {}
		memberReturnCount = 0
		sameMemberWithTime = ''
		educloud_loginhistory = educloud_loginhistory_coll.find({"city_id": city_id, "created_at": { "$gte": begin_date, "$lt": end_date}}).sort([('UserAccount',1), ('created_at',1)])
		
		educloud_loginhistoryStart2 = datetime.datetime.now()
		for loginhistory in educloud_loginhistory :
			email = loginhistory['UserAccount']
			_city_id = str(loginhistory['city_id'])
			#同一天內的登入記錄只算一筆
			#print(sameMemberWithTime + '  <-->   ' + email + loginhistory['created_at'].strftime("%Y%m%d"))
			if sameMemberWithTime != (email + loginhistory['created_at'].strftime("%Y%m%d")) :
				sameMemberWithTime = email + loginhistory['created_at'].strftime("%Y%m%d")
				formatted_SQL = SQL.format(email, loginhistory['created_at'].strftime("%Y-%m-%d %H:%M:%S"))
				memberinfo = pd.read_sql_query(formatted_SQL, conn)				
				if set([_city_id]).issubset(cityMemberReturn) == True :
					cityMemberReturn[_city_id] = cityMemberReturn.get(_city_id) + len(memberinfo)
				else :
					cityMemberReturn[_city_id] = len(memberinfo)
			else :
				continue
			#print(len(memberinfo))
		educloud_loginhistoryEnd2 = datetime.datetime.now()
		print('educloud_loginhistory2_loopDeltaTime:	{}.'.format(educloud_loginhistoryEnd2 - educloud_loginhistoryStart2))

		cityMemberReturnStart = datetime.datetime.now()
		for _city_id, returntimes in cityMemberReturn.items():
			if (returntimes > 0) :
				memberReturnCount = memberReturnCount + 1
		cityMemberReturnEnd = datetime.datetime.now()
		print('cityMemberReturn_loopDeltaTime:		{}.'.format(cityMemberReturnEnd - cityMemberReturnStart))

		print(year_month_start + ' ' +  str(_city_id) + '   ' + str(firstLoginMemberCount_ByCityMonth) + '  ' + str(memberReturnCount))
		'''
		db_marketanalysis.member_return_all.update_one(
			{'year_month' : year_month_start},{"$set":
			{'year_month' : year_month_start, 'first' : firstLoginMemberCount_ByMonth, 'return' : memberReturnCount, 'updated_at' : datetime.datetime.now() }},
			upsert=True
		)
		'''
		db_marketanalysis.member_return_city_month.update_one(
			{'year_month' : year_month_start, 'city_id' : city_id},{"$set":
			{'year_month' : year_month_start, 'city_id' : city_id, 'first' : firstLoginMemberCount_ByCityMonth, 'return' : memberReturnCount, 'updated_at' : datetime.datetime.now() }},
			upsert=True
		)
		
		year_month_start = end_date.strftime("%Y-%m")
	whileEnd2 = datetime.datetime.now()
	print('while2_loopDeltaTime:	{}.'.format(whileEnd2 - whileStart2))

cityIdEnd = datetime.datetime.now()
print('cityId_loopDeltaTime:	{}.'.format(cityIdEnd - cityIdStart))

client.close()
conn.close()