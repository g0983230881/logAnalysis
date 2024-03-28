#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：resource_link_verify.py
#功能說明：檢查教學資源是否都有資源連結或附件
#輸入參數：無
#資料來源：MySQL => resources, resources_objectfile, objectfile
#輸出結果：stdout
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
import sys
import pymysql

sys.path.append("..")
from settings import *



conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor3 = conn.cursor(cursor=pymysql.cursors.DictCursor)
#client = MongoClient(mongo_hostname, monggo_port)
#db_marketanalysis = client[mongo_dbname_marketanalysis]

ResourceSQL = "select id,title,bmtype,workflow_id,member_id from resources where isdelete='N' and workflow_id in ('WK2005','WK2008')"
ResourceObjSQL = "select * from resources_objfile where isdelete='N' and resources_id={0}"
ObjectFileSQL  = "select * from objectfile where isdelete='N' and id={0}"

cursor1.execute(ResourceSQL)
resourcesRows = cursor1.fetchall()
for resources in resourcesRows :
	resources_id = resources.get('id')

	resourcesLinkFound = False
	cursor2.execute(ResourceObjSQL.format(resources_id))
	resourcesObjRows = cursor2.fetchall()
	for resourcesobj in resourcesObjRows :
		objectfile_id = resourcesobj.get('objectfile_id')

		cursor3.execute(ObjectFileSQL.format(objectfile_id))
		objectfileRow = cursor3.fetchone()
		if objectfileRow is not None:
			resourcesLinkFound = True
		else :
			break
	if resourcesLinkFound == False :
		print('/resources/' + resources.get('bmtype') + '/' + str(resources_id) + ' ' + resources.get('workflow_id') + ' link not found!')
		
	#print(str(resources_id))
print('total:' + str(len(resourcesRows)))
conn.close()
