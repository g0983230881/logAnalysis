#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：ContributionAnalysis.py
#功能說明：分析縣市／學校上傳資源的貢獻量(不考慮下架),及個別會員上傳資源的貢獻量
#輸入參數：無
#資料來源：MySQL:edumarket -> resources
#輸出結果：
#         每天匯整的資料存放在MongoDB:marketanalysis -> contrib_city及contrib_member資料集
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from os.path import dirname
import datetime
import os,sys
import pymysql
import pandas as pd

sys.path.append("..")
from settings import *


conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
contrib_city_col = db_marketanalysis.contrib_city
contrib_member_col = db_marketanalysis.contrib_member


#統計總共有哪些天有人上傳資源
resources_createdate_groupSQL = "select date_format(created_at,'%Y-%m-%d') as createdate from resources where isdelete='N' group by date_format(created_at,'%Y-%m-%d') order by date_format(created_at,'%Y-%m-%d')"
#查詢出特定一天上傳教學資源的記錄
resources_selectByCreatedateSQL = "select id,city_id,member_id,school_id from resources where isdelete='N' and date_format(created_at,'%Y-%m-%d')='{0}'"
#查詢特定教學資源的學科資料
resources_selectDomainSQL = "select discipline_id from domain where isdelete='N' and resources_id={0}"
#查詢特定教學資源的適用年級資料
resources_selectGradeSQL = "select grade from grade where isdelete='N' and resources_id={0}"


cursor1Start = datetime.datetime.now()
totalrecord = pd.read_sql_query(resources_createdate_groupSQL, conn)
# totalrecord.head(3)
#    createdate
# 0  2004-07-30
# 1  2004-09-16
# 2  2004-12-21
cursor1End = datetime.datetime.now()
print('cursor1_deltaTime:		{}.'.format(cursor1End - cursor1Start))


for value in totalrecord.itertuples(index=False, name=None):
	##---totalrecordStart
	totalrecordStart = datetime.datetime.now()

	createDate = value[0]

	city_statist = {}
	city_school_statist = {}
	member_statist = {}
	discipline_statist = {}
	grade_statist = {}

	cursor2Start = datetime.datetime.now()
	formatted_createDate = resources_selectByCreatedateSQL.format(createDate)
	resources = pd.read_sql_query(formatted_createDate, conn)
	# print(resources.head(3))
	# 	   id  city_id  member_id  school_id
	# 0   2       51      13712          0
	# 1  15       51      13712          0
	# 2  21       51      13712          0
	cursor2End = datetime.datetime.now()
	print('cursor2_deltaTime:		{}.'.format(cursor2End - cursor2Start))
	

	##---resourcesStart
	resourcesStart = datetime.datetime.now()
	for resource in resources.itertuples(index=False, name=None):
		resources_id = str(resource[0])

		#統計每天各縣市上傳的資源筆數
		# IfStart = datetime.datetime.now()
		city_id = str(resource[1])
		if set([city_id]).issubset(city_statist) == True :
			city_statist[city_id] = city_statist.get(city_id)+1
		else :
			city_statist[city_id] = 1

		#統計每天各縣市中各個學校上傳的資源筆數
		school_id = str(resource[3])
		if (school_id != "0") : 
			if set([city_id + "_" + school_id]).issubset(city_school_statist) == True :
				city_school_statist[city_id + "_" + school_id] = city_school_statist.get(city_id + "_" + school_id)+1
			else :
				city_school_statist[city_id + "_" + school_id] = 1

		#統計每天每個人上傳資源的筆數
		member_id = str(resource[2])
		if (int(member_id) > 0) :
			if set([member_id]).issubset(member_statist) == True :
				member_statist[member_id] = member_statist.get(member_id) + 1
			else :
				member_statist[member_id] = 1
		# IfEnd = datetime.datetime.now()
		# print('If_DeltaTime:			{}.'.format(IfEnd - IfStart))

		#統計每天各縣市上傳資源的學科筆數
		# formattedLoopStart = datetime.datetime.now()
		cursor.execute(resources_selectDomainSQL.format(resources_id))
		Disciplines = cursor.fetchall()
		for _discipline in Disciplines :
			discipline_id = str(_discipline.get('discipline_id'))
			if set([city_id + "_" + discipline_id]).issubset(discipline_statist) == True :
				discipline_statist[city_id + "_" + discipline_id] = discipline_statist.get(city_id + "_" + discipline_id) + 1
			else:
				discipline_statist[city_id + "_" + discipline_id] = 1
		
		#統計每天各縣市上傳資源的適用年級筆數
		cursor.execute(resources_selectGradeSQL.format(resources_id))
		Grades = cursor.fetchall()
		for _grade in Grades :
			grade = str(_grade.get('grade'))
			if set([city_id + "_" + grade]).issubset(grade_statist) == True :
				grade_statist[city_id + "_" + grade] = grade_statist.get(city_id + "_" + grade) + 1
			else:
				grade_statist[city_id + "_" + grade] = 1
		# formattedLoopEnd = datetime.datetime.now()
		# print('formatted_loopDeltatime: 	{}'.format(formattedLoopEnd - formattedLoopStart))

	resourcesEnd = datetime.datetime.now()
	print('resources_loopDeltaTime:	{}.'.format(resourcesEnd - resourcesStart))
	##---resourcesEnd
	
	
	#將各縣市/學校的上傳資源統計結果寫出到mongo資料庫
	city_statistStart = datetime.datetime.now()
	for city_id in city_statist :
		school = []
		for k in city_school_statist :
			num = city_school_statist.get(k)
			school_id_list = k.split("_")
			if int(school_id_list[0]) == int(city_id) :
				school.append({'school_id' : school_id_list[1], 'num' : num})

		discipline = []
		for k in discipline_statist :
			num = discipline_statist.get(k)
			discipline_id_list = k.split("_")
			if int(discipline_id_list[0]) == int(city_id) :
				discipline.append({'discipline_id' : discipline_id_list[1], 'num' : num})

		grade = []
		for k in grade_statist :
			num = grade_statist.get(k)
			grade_id_list = k.split("_")
			if int(grade_id_list[0]) == int(city_id) :
				grade.append({'grade' : grade_id_list[1], 'num' : num})

		contrib_city_col.update_one(
			{'upload_date' : createDate, 'city_id' : city_id},{"$set":
			{'upload_date' : createDate, 'city_id' : city_id, 'year_month' : createDate[0:7], 'num' : city_statist.get(city_id), 'school' : school, 
			 'discipline' : discipline, 'grade' : grade, 'updated_at' : datetime.datetime.now()}},
			upsert=True
		)
	city_statistEnd = datetime.datetime.now()
	print('city_statist_loopDeltaTime:	{}.'.format(city_statistEnd - city_statistStart))
	##---city_statistEnd


	#將每天每個人上傳資源筆數的統計結果寫出到mongo資料庫
	##---member_statistStart
	member_statistStart = datetime.datetime.now()
	for k in member_statist :
		contrib_member_col.update_one(
			{'upload_date' : createDate, 'member_id' : int(k)},{"$set":
			{'upload_date' : createDate, 'member_id' : int(k), 'num' : member_statist.get(k), 'updated_at' : datetime.datetime.now()}},
			upsert=True
			)
	member_statistEnd = datetime.datetime.now()
	print('member_statist_loopDeltaTime:	{}.'.format(member_statistEnd - member_statistStart))
	##---member_statistEnd

	if 'upload_date' not in contrib_city_col.index_information() :
		contrib_city_col.create_index('upload_date')
	if 'city_id' not in contrib_city_col.index_information() :
		contrib_city_col.create_index('city_id')
	if 'year_month' not in contrib_city_col.index_information() :
		contrib_city_col.create_index('year_month')
	if 'upload_date' not in contrib_member_col.index_information() :
		contrib_member_col.create_index('upload_date')
	if 'member_id' not in contrib_member_col.index_information() :
		contrib_member_col.create_index('member_id')
	
	totalrecordEnd = datetime.datetime.now()
	print('totalrecord_loopDeltaTime:	{}.'.format(totalrecordEnd - totalrecordStart))
	##---totalrecordEnd


#將沒有查出上傳資源那天的統計資料予以刪除
#2017/08/27, fann, 改從更新日期判斷是否資料有被異動,沒有異動表示哪天的資料已不存在,故將資料清除
'''
print('Delete empty data by date')
if (len(totalrecord) >=0) :
	start_date = datetime.datetime(2004, 1, 1)
	end_date = datetime.datetime.now().strftime('%Y-%m-%d')
	while start_date.strftime('%Y-%m-%d') != end_date:
		#print(start_date)
		if (start_date not in totalrecord) :
			contrib_city_col.delete_many({'upload_date' : start_date})
			contrib_member_col.delete_many({'upload_date' : start_date})
		start_date = start_date + datetime.timedelta(days = 1)  # 每次增加一天
'''
last_date = (datetime.datetime.now() -  datetime.timedelta(hours=24)).strftime('%Y-%m-%d')
contrib_city_col.delete_many({'updated_at' : {'$lt' : last_date}})
contrib_member_col.delete_many({'updated_at' : {'$lt' : last_date}})
client.close()
conn.close()


currentDirectory = dirname(__file__)
if len(currentDirectory) == 0 :
	currentDirectory = "."

os.system(currentDirectory + "/ContributionAnalysis_month.py")