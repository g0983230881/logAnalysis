#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：resource_duplicate_detect.py
#功能說明：檢查教學資源是否重複(比對標題,及描述說明)
#輸入參數：無
#資料來源：MySQL => resources
#輸出結果：resource_duplicate.txt
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from importlib import reload
import sys
import pymysql

sys.path.append("..")
from settings import *


reload(sys)

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor3 = conn.cursor(cursor=pymysql.cursors.DictCursor)
#client = MongoClient(mongo_hostname, monggo_port)
#db_marketanalysis = client[mongo_dbname_marketanalysis]

ResourceSQL = "select id,title,`desc`,bmtype,workflow_id,member_id from resources where isdelete='N' and workflow_id in ('WK2005','WK2008')"
SQL = "select id,title,`desc`,bmtype,workflow_id,member_id from resources where isdelete='N' and workflow_id in ('WK2005','WK2008') and title=%s and `desc`=%s order by id"

resourcesIdList = {}
resfile1 = open("../output/resource_duplicate.txt","w+")
cursor1.execute(ResourceSQL)
resourcesRows = cursor1.fetchall()
for resources in resourcesRows :
	resources_id = resources.get('id')
	title = resources.get('title')
	desc = resources.get('desc')

	try :
		resourcesId = ''
		cursor2.execute(SQL, (title, desc))
		maybeDuplicateResourceRows = cursor2.fetchall()
		for dupResources in maybeDuplicateResourceRows :
			resourcesId += str(dupResources.get('id')) + ' '

		if resourcesId != '' and len(maybeDuplicateResourceRows)>1 :
			if set([resourcesId]).issubset(resourcesIdList) == False :
				resourcesIdList[resourcesId] = 1
				resfile1.write(resourcesId + '\n')
	except :
		print(str(resources_id) + ' find error!')
resfile1.close()
conn.close()
