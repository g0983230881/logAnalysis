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
from calendar import monthrange
import datetime
import sys
import pymysql

sys.path.append("..")
from settings import *

def tryInt(source):
	try:
		return int(source)
	except:
		return 0


conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]

#上架
SQL1 = "select count(*) total from resources where workflow_id='WK2005' and isdelete='N' and last_publish_time<='{0}'"
#下架
SQL2 = "select count(*) total from resources where workflow_id='WK2007' and isdelete='N' and last_publish_time<='{0}'"
#退回
SQL3 = "select count(*) total from resources where workflow_id='WK2006' and isdelete='N' and last_publish_time<='{0}'"
#連結失效
SQL4 = "select count(*) total from resources where workflow_id='WK2008' and isdelete='N' and last_publish_time<='{0}'"
now = datetime.datetime.now()
firstRecordDate = list(db_marketanalysis.search_log2.aggregate([{'$group':{'_id':'', 'last':{'$min':"$since"}}}]))[0]['last']
year_month_start = firstRecordDate.strftime("%Y-%m")
while now.strftime("%Y-%m") >= year_month_start :
	begin_date = datetime.datetime.strptime(year_month_start+'-01', "%Y-%m-%d")
	end_date = begin_date + relativedelta(months=1)
	#year = int(firstRecordDate.strftime("%Y"))
	#month = int(firstRecordDate.strftime("%m"))
	#month_lastday = monthrange(year, month)[1]

	docs_rsc_usage = [doc for doc in db_marketanalysis.search_log2.find(
		{"class_code": { "$in": ["R", "M_R"]},"since": { "$gt": begin_date + datetime.timedelta(hours=-8), "$lte": end_date + datetime.timedelta(hours=-8)}}, {"main_col": 1})]

	rsc_ids = filter(lambda x: x!=0, [tryInt(doc['main_col']) for doc in docs_rsc_usage]) # filter error number (as 0)
	rsc_usage_group = Counter(rsc_ids) # dictionary: [{ rid: usage_count }, ...]

	#zero_usage = len(set(all_rid) - set(rsc_usage_group.keys())) # all resources_id disjoin usage resources_id
	onceOrmore_usage = len({k:v for k,v in rsc_usage_group.items() if v > 0})

	cursor1.execute(SQL1.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
	publishResources = cursor1.fetchone()
	cursor1.execute(SQL2.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
	unPublishResources = cursor1.fetchone()
	cursor1.execute(SQL3.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
	rejectResources = cursor1.fetchone()
	cursor1.execute(SQL4.format(end_date.strftime('%Y-%m-%d %H:%M:%S')))
	linkfailResources = cursor1.fetchone()

	db_marketanalysis.resources_usage_all.update_one(
		{'year_month': begin_date.strftime("%Y-%m") },{"$set":
		{'year_month': begin_date.strftime("%Y-%m"), 'publish': publishResources['total'], 'unpublish':unPublishResources['total'], 
		 'reject':rejectResources['total'], 'linkfail':linkfailResources['total'], 'usage' : onceOrmore_usage, 'updated_at' : datetime.datetime.now() }},
		upsert=True
	)

	print(begin_date.strftime('%Y-%m-%d %H:%M:%S') + ' ~ ' + end_date.strftime('%Y-%m-%d %H:%M:%S') + '  ' + str(onceOrmore_usage))
	year_month_start = end_date.strftime("%Y-%m")

client.close()
conn.close()
