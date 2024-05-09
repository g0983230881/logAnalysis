#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_growing.py
#功能說明：統計每個月各縣市以教育雲單一簽入帳號,首次登入到教育大市集的會員增長數量
#輸入參數：無
#資料來源：MySQL的member資料表
#輸出結果：統計結果寫入到marketanalysis資料庫的member_growing資料集
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
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]
member_growing_col = db_marketanalysis.member_growing
member_total_col = db_marketanalysis.member_growing_all

#區分縣市統計各縣市每個月的會員增加數量
SQL = "select date_format(created_at, '%Y-%m') yearmonth,city_id,count(*) total from member where idtype='E' group by yearmonth,city_id"
cursor1.execute(SQL)
member_growing_all = cursor1.fetchall()
for memberCount_byCityWithMonth in member_growing_all :
	year_month = memberCount_byCityWithMonth.get('yearmonth')
	city_id = memberCount_byCityWithMonth.get('city_id')
	count = memberCount_byCityWithMonth.get('total')

	member_growing_col.update_one(
			{'year_month' : year_month, 'city_id' : city_id},
			{'$set' : {'year_month' : year_month, 'city_id' : city_id, 'count' : count, 'updated_at' : datetime.datetime.now()}},
			upsert=True
			)
print('區分縣市統計各縣市每個月的會員增加數量 finish')

#區分縣市統計各縣市每個月的會員數量
SQL = "select date_format(created_at, '%Y-%m') yearmonth,count(*) total from member where idtype='E' and date_format(created_at, '%Y-%m')<='{0}' and city_id={1} group by yearmonth"
yearMonthList = member_growing_col.distinct('year_month')
cityIdList = member_growing_col.distinct('city_id');
for year_month in yearMonthList:
	for city_id in cityIdList :
		cursor1.execute(SQL.format(year_month, city_id))
		member_growing_city_month = cursor1.fetchall()
		count = 0
		for memberCount_byCityWithMonth in member_growing_city_month :
			count += memberCount_byCityWithMonth.get('total')

		member_growing_col.update_one(
						{'year_month' : year_month, 'city_id' : city_id},
					   	{'$set' : {'total' : count, 'updated_at' : datetime.datetime.now()}},
					upsert=False
				)
print('區分縣市統計各縣市每個月的會員數量 finish')

#回補登入者的縣市代碼,以利進行後續的分析
SQL = "select * from member where idtype='E' and userid='cloudid://{0}'"
loginHistory = db_edumarket.educloud_loginhistory.find()
for login in loginHistory :
	city_id = 0
	_id = login.get('_id')
	created_at = login.get('created_at');

	if login.get('city_id') == None or login.get('city_id') == 0 :	
		UserAccount = login.get('UserAccount')
		cursor1.execute(SQL.format(UserAccount))
		member = cursor1.fetchone()
		if member != None :
			city_id = member.get('city_id')
	else :
		city_id = login.get('city_id')

	db_edumarket.educloud_loginhistory.update_one(
		{'_id' : _id},
		{'$set' : {'city_id' : city_id, 'year_month' : created_at.strftime('%Y-%m')}},
		upsert=False
	)
print('回補登入者的縣市代碼,以利進行後續的分析 finish')

member_growing = member_growing_col.find()
for memberGrowing in member_growing :	
	loginMember = {}
	logintime = 0
	city_id = memberGrowing.get('city_id')
	year_month = memberGrowing.get('year_month')
	loginHistory = db_edumarket.educloud_loginhistory.find({'city_id':city_id, 'year_month' : year_month})
		#remove duplicate login user
	for login in loginHistory :
		logintime = logintime + 1
		useraccount = login.get('UserAccount')
		if set([useraccount]).issubset(loginMember) == False :
						loginMember[useraccount] = login

	#print(year_month + '  ' + str(city_id) + ' ---> ' + str(len(loginMember)))
		member_growing_col.update_one(
				{'year_month' : year_month, 'city_id' : city_id},
				{'$set' : {'usesso' : len(loginMember), 'logintime' : logintime, 'updated_at' : datetime.datetime.now()}},
				upsert=False
		)
print('統計縣市會員登入量 finish')

#不區分縣市統計全站的會員成長數量
total = 0
SQL = "select count(*) total from member where idtype='E' and date_format(created_at, '%Y-%m') = '{0}'"
yearMonthList = member_growing_col.distinct('year_month')
for year_month in yearMonthList:
	cursor1.execute(SQL.format(year_month))
	member_total = cursor1.fetchall()
	for memberTotal in member_total :
		count = memberTotal.get('total');
		total+= count;
		member_total_col.update_one(
			{'year_month' : year_month},
			{'$set' : {'year_month' : year_month, 'count' : count, 'total': total, 'updated_at' : datetime.datetime.now()}},
			upsert=True
		)
print('#不區分縣市統計全站的會員成長數量 finish')

#分析每個月活動會員及活躍性會員的數量
nowdate = datetime.datetime.now()
beforeOneYearDate = nowdate + relativedelta(years=-1)

activeUserList = db_edumarket.educloud_loginhistory.distinct("UserId", {"created_at" : {"$gte":beforeOneYearDate,"$lt":nowdate}})
heavyUserList = db_edumarket.educloud_loginhistory.aggregate(
   [
	  {
		'$match' : {
		   'created_at':{'$gte':beforeOneYearDate, '$lt':nowdate}
		},
	  },
	  {
		'$group' : {
		   '_id' : { 'UserId': "$UserAccount" },
		   'count': { '$sum': 1 }
		}
	  },
	  { '$sort' : { 'count' : -1 } }
   ]
)
totalCount = 0;
for doc in list(heavyUserList) :
	 if (doc['count'] > 3) :
		 totalCount= totalCount+1; 

print(beforeOneYearDate.strftime("%Y-%m-%d") + ' --- ' + nowdate.strftime("%Y-%m-%d") + '  ' + str(len(activeUserList))   + '  ' + str(totalCount))
member_total_col.update_one(
			{'year_month' : nowdate.strftime("%Y-%m")},
			{'$set' : {'active' : len(activeUserList), 'heavey': totalCount, 'updated_at' : datetime.datetime.now()}},
			upsert=True
		)
print('分析每個月活動會員及活躍性會員的數量 finish')

nowdate = datetime.datetime(nowdate.year, nowdate.month, 1)
while (nowdate.strftime("%Y-%m") >= '2017-02') :		
	activeUserList = db_edumarket.educloud_loginhistory.distinct("UserId", {"created_at" : {"$gte":(nowdate + relativedelta(years=-1)),"$lt":nowdate}})

	heavyUserList = db_edumarket.educloud_loginhistory.aggregate(
	  [
		 {
		   '$match' : {
			   'created_at':{'$gte':(nowdate + relativedelta(years=-1)), '$lt':nowdate}
		   },
		 },
		 {
		   '$group' : {
			  '_id' : { 'UserId': "$UserAccount" },
			  'count': { '$sum': 1 }
		   }
		 },
		 { '$sort' : { 'count' : -1 } }
	  ]
	)
	totalCount = 0;
	for doc in list(heavyUserList) :
		if (doc['count'] > 3) :
			totalCount= totalCount+1;  

	print((nowdate + relativedelta(years=-1)).strftime("%Y-%m-%d") + ' --- ' + nowdate.strftime("%Y-%m-%d") + ' -> ' + str(len(activeUserList))   + '  ' + str(totalCount))
	member_total_col.update_one(
			{'year_month' : (nowdate + relativedelta(months=-1)).strftime("%Y-%m")},
			{'$set' : {'active' : len(activeUserList), 'heavey': totalCount, 'updated_at' : datetime.datetime.now()}},
			upsert=True
		)
	nowdate = nowdate + relativedelta(months=-1)




conn.close()
client.close()






