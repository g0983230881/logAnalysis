#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：
#功能說明：更新workflow_id 回資料表
#輸入參數：無
#資料來源：edumarket -> resources
#輸出結果：update mysql

from pymongo import MongoClient
import sys
import pymysql

sys.path.append("..")
from settings import *

client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]

conn = pymysql.connect(host=mysql_hostname, database=mysql_dbname, user=mysql_user, passwd=mysql_password, charset='utf8')
conn_origin = pymysql.connect(host=mysql_hostname, database=mysql_dbname, user=mysql_user, passwd=mysql_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)  ## edumarket_latest
cursor_origin = conn_origin.cursor(cursor=pymysql.cursors.DictCursor)  ## edumarket


# ## 查詢 mongodb > resources資料表中 所有workflow_id資訊, 更新回mysql > resources
# resources_datas = db_edumarket.resources.find()
# for resources_data in resources_datas:
# 	resources_id = resources_data['id']
# 	workflow_id = resources_data['workflow_id']
	
# SQL = "UPDATE resources SET workflow_id = '%s' WHERE id = '%s' "%(workflow_id, resources_id)
# 	cursor.execute(SQL)
# 	conn.commit()
# 	print(cursor.rowcount)

# cursor.close()
# client.close()	


## 查詢 mysql > edumarket > resources資料表中 所有workflow_id資訊, 更新回mysql > edumarket_latest > resources
SQL = 'select id, workflow_id from resources'
cursor_origin.execute(SQL)
resources_datas = cursor_origin.fetchall()
for resources_data in resources_datas:
	resources_id = resources_data.get('id')
	workflow_id = resources_data.get('workflow_id')
	
	SQL1 = "UPDATE resources SET workflow_id = '%s' WHERE id = '%s' "%(workflow_id, resources_id)
	cursor.execute(SQL1)
	conn.commit()
	print(cursor.rowcount)

cursor.close()
cursor_origin.close()