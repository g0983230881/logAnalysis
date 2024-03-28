#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：ContributionAnalysis_month.py
#功能說明：匯整各縣市每天上傳教學資源的數據成為月份的數據
#輸入參數：無
#資料來源：MongoDB:marketanalysis -> contrib_city
#輸出結果：MongoDB:marketanalysis -> contrib_city_month, contrib_all
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
import datetime
import sys
import pymysql
import calendar

sys.path.append("..")
from settings import *


def month_sub(year, month, sub_month):
	result_month = 0
	result_year = 0
	if month > (sub_month % 12):
		result_month = month - (sub_month % 12)
		result_year = year - (sub_month // 12)
	else:
		result_month = 12 - (sub_month % 12) + month
		result_year = year - (sub_month // 12 + 1)
	return (result_year, result_month)


def month_add(year, month, add_month):
	return month_sub(year, month, -add_month)


def statist_ContributeByCityMonth(city_Id, year_month) :
	statist_month = ""
	statist_cityid =''
	num = 0
	school = []
	grade = []
	discipline = []
	hasDataToWrite = False
	city_id = ''
	SQLcount_ByMonth = "select count(*) as total from resources where isdelete='N' and created_at <= '{0}'"
	SQLcount_ByCity_Month = "select count(*) as total from resources where isdelete='N' and city_id={0} and created_at <= '{1}'"
	#print(city_Id + ' - ' + year_month)

	contrib_city = contrib_city_col.find({'city_id':city_Id, 'year_month':{'$lte':year_month}}).sort([('year_month', 1),('city_id', 1)])
	#print(city_Id + '--' + year_month + '--->' + str(contrib_city.count()))
	for _contrib_city in contrib_city :
		city_id = _contrib_city['city_id']
		upload_date = _contrib_city['year_month'] 
		upload_month = upload_date[0:7]
		hasDataToWrite = True
		statist_month = year_month
		statist_cityid = city_Id

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
		last_day = str(calendar.monthrange(int(statist_month.split("-")[0]), int(statist_month.split("-")[1]))[1])
		cursor1.execute(SQLcount_ByCity_Month.format(statist_cityid, statist_month+"-"+last_day+" 12:59:59"))
		city_countResult = cursor1.fetchone()
		cursor2.execute(SQLcount_ByCity_Month.format(statist_cityid, statist_month+"-01 00:00:00"))
		city_lastMonthCountResult = cursor2.fetchone()

		#計算不分縣市的資源上傳總筆數－月底及月初
		cursor1.execute(SQLcount_ByMonth.format(statist_month+"-"+last_day+" 12:59:59"))
		thisMonthCountResult = cursor1.fetchone()
		cursor1.execute(SQLcount_ByMonth.format(statist_month+"-01 00:00:00"))
		lastMonthCountResult = cursor1.fetchone()


		db_marketanalysis.contrib_all.update_one(
			{'year_month' : statist_month},{"$set":
			{'year_month' : statist_month, 'count':thisMonthCountResult['total']-lastMonthCountResult['total'], 'total' : thisMonthCountResult['total']}},
			upsert=True
		)

		contrib_city_month_col.update_one(
			{'year_month' : statist_month, 'city_id' : statist_cityid},{"$set":
			{'year_month' : statist_month, 'city_id' : statist_cityid, 'num' : num, 'school' : school,			 
			 'discipline' : discipline, 'grade' : grade, 'total_thismonth' : city_countResult['total'], 'total_lastmonth': city_lastMonthCountResult['total']}},
			upsert=True
		)



conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
contrib_city_col = db_marketanalysis.contrib_city
contrib_city_month_col = db_marketanalysis.contrib_city_month

now = datetime.datetime.now()
cityId = contrib_city_col.distinct('city_id')
for city_id in cityId:
	#print(city_id)
	contrib_startDate = contrib_city_col.find({'city_id':city_id}).sort([('year_month', 1)]).limit(1)
	contrib_startYear = int(contrib_startDate[0]['year_month'][0:4])
	contrib_startMonth = int(contrib_startDate[0]['year_month'][5:])
	#contrib_YearMonth = contrib_startDate[0]['year_month'][0:4] + contrib_startDate[0]['year_month'][5:]

	while int(str(contrib_startYear) + '{0:02d}'.format(contrib_startMonth)) < int(str(now.year) + '{0:02d}'.format(now.month)) :

		contrib_startYear, contrib_startMonth = month_add(contrib_startYear, contrib_startMonth, 1)
		print(city_id + '  ' + str(contrib_startYear) + '-' + '{0:02d}'.format(contrib_startMonth))
		statist_ContributeByCityMonth(city_id, str(contrib_startYear) + '-' + '{0:02d}'.format(contrib_startMonth))

	#print(city_id + '  ' + contrib_startDate[0]['year_month'] + str(month_add(int(_startYearMonth[0:4]), int(_startYearMonth[6:8]), 1)))
client.close()
conn.close()
