#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：ContributionAnalysis_month.py
#功能說明：匯整各縣市每天上傳教學資源的數據成為月份的數據
#輸入參數：無
#資料來源：MongoDB:marketanalysis -> contrib_city
#輸出結果：MongoDB:marketanalysis -> contrib_city_month
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
import sys
import pymysql

sys.path.append("..")
from settings import *


conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
contrib_city_col = db_marketanalysis.contrib_city
contrib_city_month_col = db_marketanalysis.contrib_city_month

statist_month = ""
statist_cityid =''
num = 0
school = []
grade = []
discipline = []
hasDataToWrite = False
city_id = ''
SQLcount_ByMonth = "select count(*) as total from resources where isdelete='N' and created_at <= '{1}'"
SQLcount_ByCity_Month = "select count(*) as total from resources where isdelete='N' and city_id={0} and created_at <= '{1}'"
cityId = contrib_city_col.distinct('city_id')
for city_id in cityId:
	print(city_id)

'''
contrib_city = contrib_city_col.find({}).sort([('year_month', 1),('city_id', 1)])
for _contrib_city in contrib_city :
	city_id = _contrib_city['city_id']
	upload_date = _contrib_city['year_month'] 
	upload_month = upload_date[0:7]
	hasDataToWrite = True

	#初始化統計的月份變數，將第一筆資料的月份設為統計的月份
	if statist_month == '' :
		statist_month = upload_month		
		statist_cityid = city_id
        
	if statist_month!=upload_month or statist_cityid!=city_id:
		cursor1.execute(SQLcount_ByCity_Month.format(statist_cityid, statist_month+"-31 12:59:59"))
		countResult = cursor1.fetchone()
                cursor2.execute(SQLcount_ByCity_Month.format(statist_cityid, statist_month+"-01 00:00:00"))
                lastMonthCountResult = cursor2.fetchone()
                cursor1.execute(SQLcount_ByMonth.format(statist_cityid, statist_month+"-31 12:59:59"))
                thisMonthCountResult = cursor1.fetchone()  
		#統計的月份若與目前讀取資料的月份不同,則將統計的資料寫出,並以目前的月份為下一回的統計月份
                print(statist_month + " " + statist_cityid + " " + str(countResult['total']) + " " + str(lastMonthCountResult['total']))
		#print(statist_month + " " + statist_cityid + " " + str(countResult['total']) + "  " + SQLcount_ByCity_Month.format(statist_cityid, statist_month))
		#print(school)
		#print(grade)
                db_marketanalysis.contrib_all.update_one(
			{'year_month' : statist_month},{"$set":
			{'year_month' : statist_month, 'count' : thisMonthCountResult['total']}},
			upsert=True
		)
		contrib_city_month_col.update_one(
			{'year_month' : statist_month, 'city_id' : statist_cityid},{"$set":
			{'year_month' : statist_month, 'city_id' : statist_cityid, 'num' : num, 'school' : school, 
			 'discipline' : discipline, 'grade' : grade, 'total_thismonth' : countResult['total'], 'total_lastmonth': lastMonthCountResult['total']}},
			upsert=True
			)
		
		if 'year_month' not in contrib_city_month_col.index_information() :
			contrib_city_month_col.create_index('year_month')

		statist_month = upload_month
                statist_cityid = city_id
		num = _contrib_city['num'];
		school = _contrib_city['school']
		grade = _contrib_city['grade']
		discipline = _contrib_city['discipline']
	else :
		#hasDataToWrite = True
		num+=_contrib_city['num']
		#匯整學校的統計數據
		for item in _contrib_city['school'] :
			found = False
			for schoolitem in school :
				if schoolitem['school_id'] == item['school_id'] :
					schoolitem['num']+=item['num']
					found = True
			if found == False :
				school.append(item)

		#匯整適用年級的統計數據
		for item in _contrib_city['grade'] :
			found = False
			for gradeitem in grade :
				if gradeitem['grade'] == item['grade'] :
					gradeitem['num']+=item['num']
					found = True
			if found == False :
				grade.append(item)

		#匯整適用學科的統計數據
		for item in _contrib_city['discipline'] :
			found = False
			for disciplineitem in discipline :
				if disciplineitem['discipline_id'] == item['discipline_id'] :
					disciplineitem['num']+=item['num']
					found = True
			if found == False :
				discipline.append(item)

#將最後還沒有寫入資料庫的資料寫入
if hasDataToWrite == True :
	cursor1.execute(SQLcount_ByCity_Month.format(statist_cityid, statist_month+"-31 12:59:59"))
	countResult = cursor1.fetchone()
	cursor2.execute(SQLcount_ByCity_Month.format(statist_cityid, statist_month+"-01 00:00:00"))
	lastMonthCountResult = cursor2.fetchone()
	cursor1.execute(SQLcount_ByMonth.format(statist_cityid, statist_month+"-31 12:59:59"))
        thisMonthCountResult = cursor1.fetchone()

	db_marketanalysis.contrib_all.update_one(
		{'year_month' : statist_month},{"$set":
                {'year_month' : statist_month, 'count' : thisMonthCountResult['total']}},
                upsert=True
                )

	contrib_city_month_col.update_one(
	        {'year_month' : statist_month, 'city_id' : statist_cityid},{"$set":
                {'year_month' : statist_month, 'city_id' : statist_cityid, 'num' : num, 'school' : school,                         
                 'discipline' : discipline, 'grade' : grade, 'total_thismonth' : countResult['total'], 'total_lastmonth': lastMonthCountResult['total']}},
                upsert=True
                )
'''
client.close()
conn.close()
