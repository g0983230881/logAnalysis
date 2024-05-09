#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：resource_usage.py
#功能說明：匯整教學資源使用量
#輸入參數：無
#資料來源：marketanalysis -> search_log2
#輸出結果：marketanalysis -> resources_usage_all
#開發人員：Chi-Wen Fann

from collections import Counter
from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from calendar import monthrange
import datetime
import sys
import pymysql 
import pandas as pd

sys.path.append("..")
from settings import *

def tryInt(source):
	try:
		return int(source)
	except:
		return 0

# sqlconnStart = datetime.datetime.now()
# conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
# cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
engine = create_engine('mysql+pymysql://{user}:{password}@{hostname}/{dbname}'.format(
    user=mysqlRead_user,
    password=mysqlRead_password,
    hostname=mysqlRead_hostname,
    dbname=mysqlRead_dbname,
	charset='utf8'
))
client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
# sqlconnEnd = datetime.datetime.now()
# print('sqlconnDeltatime: 	{}'.format(sqlconnEnd - sqlconnStart))

#上架
SQL1 = "select count(*) total from resources where workflow_id='WK2005' and isdelete='N' and last_publish_time<='{0}'"
#下架
SQL2 = "select count(*) total from resources where workflow_id='WK2007' and isdelete='N' and last_publish_time<='{0}'"
#退回
SQL3 = "select count(*) total from resources where workflow_id='WK2006' and isdelete='N' and last_publish_time<='{0}'"
#連結失效
SQL4 = "select count(*) total from resources where workflow_id='WK2008' and isdelete='N' and last_publish_time<='{0}'"
now = datetime.datetime.now()
# firstRecordStart = datetime.datetime.now()
firstRecordDate = list(db_marketanalysis.search_log2.aggregate([{'$group':{'_id':'', 'last':{'$min':"$since"}}}]))[0]['last']
year_month_start = firstRecordDate.strftime("%Y-%m")
print('firstRecordDate aggregate finished.')
# firstRecordEnd = datetime.datetime.now()
# print('firstRecordDeltatime: 	{}'.format(firstRecordEnd - firstRecordStart))

while now.strftime("%Y-%m") >= year_month_start :
	begin_date = datetime.datetime.strptime(year_month_start+'-01', "%Y-%m-%d")
	end_date = begin_date + relativedelta(months=1)
	#year = int(firstRecordDate.strftime("%Y"))
	#month = int(firstRecordDate.strftime("%m"))
	#month_lastday = monthrange(year, month)[1]

	docs_rsc_usage = [doc for doc in db_marketanalysis.search_log2.find(
		{"class_code": { "$in": ["R", "M_R"]},"since": { "$gt": begin_date + datetime.timedelta(hours=-8), "$lte": end_date + datetime.timedelta(hours=-8)}}, {"main_col": 1})]

	# type(rsc_ids): list
	rsc_ids = filter(lambda x: x!=0, [tryInt(doc['main_col']) for doc in docs_rsc_usage]) # filter error number (as 0) 
	# type(rsc_usage_group): dict
	rsc_usage_group = Counter(rsc_ids) # dictionary: [{ rid: usage_count }, ...]

	#zero_usage = len(set(all_rid) - set(rsc_usage_group.keys())) # all resources_id disjoin usage resources_id
	onceOrmore_usage = len({k:v for k,v in rsc_usage_group.items() if v > 0})

	# cursor1Start = datetime.datetime.now()
	publishResources_df = pd.read_sql_query(SQL1.format(end_date), engine)
	unPublishResources_df = pd.read_sql_query(SQL2.format(end_date), engine)
	rejectResources_df = pd.read_sql_query(SQL3.format(end_date), engine)
	linkfailResources_df = pd.read_sql_query(SQL4.format(end_date), engine)
	# print(xxx_df.columns.values)
	# [u'total']
	# [u'total']
	# [u'total']
	# [u'total']
	# cursor1End = datetime.datetime.now()
	# print('cursor1 deltatime: 			{}'.format(cursor1End - cursor1Start))
	
	db_marketanalysis.resources_usage_all.update_one(
		{'year_month': begin_date.strftime("%Y-%m") },{"$set":
		{'year_month': begin_date.strftime("%Y-%m"), 'publish': int(publishResources_df.iloc[0, 0]), 'unpublish':int(unPublishResources_df.iloc[0, 0]), 
		 'reject': int(rejectResources_df.iloc[0, 0]), 'linkfail': int(linkfailResources_df.iloc[0, 0]), 'usage' : onceOrmore_usage, 'updated_at' : datetime.datetime.now() }},
		upsert=True
	)

	print(begin_date.strftime('%Y-%m-%d %H:%M:%S') + ' ~ ' + end_date.strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(onceOrmore_usage))
	year_month_start = end_date.strftime("%Y-%m")

client.close()
engine.dispose()
print('resources_usage finished.')