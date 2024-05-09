#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_behavior_countall.py
#功能說明：匯整會員個人資源相關統計
#輸入參數：無
#資料來源：mysql -> resources
#輸出結果：marketanalysis -> member_behavior_count

from pymongo import MongoClient
import datetime
import sys
import pymysql

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
db_edumarket = client[mongo_dbname_edumarket]

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

SQL = 'select id from member'
cursor.execute(SQL)

## 找出所有 會員id
memberid_datas = cursor.fetchall()
for memberid_data in memberid_datas:
	member_id = memberid_data.get('id')
	print(member_id)
	
	## 找出所有上傳資源狀態筆數
	SQL1 = 'select workflow_id,count(*) from resources where member_id=' + str(member_id) + ' and isdelete="N" group by workflow_id'
	cursor.execute(SQL1)
	resources_datas = cursor.fetchall()
	if (cursor.rowcount == 0):
		db_marketanalysis.member_behavior_count.update_one(
						{'user_id': member_id},
						{'$set': {'user_id': member_id, 'upload_resource_count': 0, 'online_resource_count': 0,
						 'edit_resource_count': 0, 'review_resource_count': 0, 'offline_resource_count': 0}},
						upsert=True
					)
	else:
		workflow_dict = {}
		process_dict = {}
		update_dict = {}
		for resources_data in resources_datas:
			workflow_id = resources_data.get('workflow_id')
			workflow_dict[workflow_id] = resources_data.get('count(*)')
		
		if 'WK0001' in workflow_dict:
			process_dict['WK0001'] = workflow_dict['WK0001']
		else:
			process_dict['WK0001'] = 0

		if 'WK1001' in workflow_dict:
			process_dict['WK1001'] = workflow_dict['WK1001']
		else:
			process_dict['WK1001'] = 0

		if 'WK2001' in workflow_dict:
			process_dict['WK2001'] = workflow_dict['WK2001']
		else:
			process_dict['WK2001'] = 0

		if 'WK2002' in workflow_dict:
			process_dict['WK2002'] = workflow_dict['WK2002']
		else:
			process_dict['WK2002'] = 0

		if 'WK2003' in workflow_dict:
			process_dict['WK2003'] = workflow_dict['WK2003']
		else:
			process_dict['WK2003'] = 0

		if 'WK2004' in workflow_dict:
			process_dict['WK2004'] = workflow_dict['WK2004']
		else:
			process_dict['WK2004'] = 0

		if 'WK2005' in workflow_dict:
			process_dict['WK2005'] = workflow_dict['WK2005']
		else:
			process_dict['WK2005'] = 0

		if 'WK2007' in workflow_dict:
			process_dict['WK2007'] = workflow_dict['WK2007']
		else:
			process_dict['WK2007'] = 0

		if 'WK2008' in workflow_dict:
			process_dict['WK2008'] = workflow_dict['WK2008']
		else:
			process_dict['WK2008'] = 0

		# update_dict['user_id'] = member_id
		# update_dict['edit_resource_count'] = process_dict['WK0001'] + process_dict['WK1001']
		# update_dict['online_resource_count'] = process_dict['WK2005']
		# update_dict['review_resource_count'] = process_dict['WK2001'] + process_dict['WK2002'] + process_dict['WK2003'] + process_dict['WK2004']
		# update_dict['offline_resource_count'] = process_dict['WK2007'] + process_dict['WK2008']
		# update_dict['upload_resource_count'] = process_dict['WK0001'] + process_dict['WK1001'] + process_dict['WK2001'] + process_dict['WK2002'] + process_dict['WK2003'] + process_dict['WK2004'] + process_dict['WK2005'] + process_dict['WK2007'] + process_dict['WK2008']
		# update_dict['user_id'] = member_id
  
		update_dict = {
				'$set': {
				'user_id': member_id,
				'edit_resource_count': process_dict['WK0001'] + process_dict['WK1001'],
				'online_resource_count': process_dict['WK2005'],
				'review_resource_count': process_dict['WK2001'] + process_dict['WK2002'] + process_dict['WK2003'] + process_dict['WK2004'],
				'offline_resource_count': process_dict['WK2007'] + process_dict['WK2008'],
				'upload_resource_count': process_dict['WK0001'] + process_dict['WK1001'] + process_dict['WK2001'] + process_dict['WK2002'] + process_dict['WK2003'] + process_dict['WK2004'] + process_dict['WK2005'] + process_dict['WK2007'] + process_dict['WK2008'],
				'user_id': member_id,
			}
		}
		
		
		db_marketanalysis.member_behavior_count.update_one(
						{'user_id': member_id},
						update_dict,
						upsert=True
					)

	## 找出該使用者上傳資源被點讚 分享 下載次數
	click_count = 0
	share_count = 0
	download_count = 0

	SQL2 = 'select id from resources where member_id=' + str(member_id)
	cursor.execute(SQL2)
	resources_datas = cursor.fetchall()
	if (cursor.rowcount == 0):
		db_marketanalysis.member_behavior_count.update_one(
						{'user_id': member_id},
						{'$set': {'user_id': member_id, 'resource_click_count': 0, 'resource_share_count': 0, 'resource_download_count': 0}},
						upsert=True
					)
	else:
		resourcesid_lst = []
		for resources_data in resources_datas:
			resourcesid_lst.append(resources_data.get('id')) 

		restatist_datas = db_edumarket.restatist.find({'resources_id': {'$in': resourcesid_lst}})
		for restatist_data in restatist_datas:
			click_count += restatist_data['url_click']
			click_count += restatist_data['click']
			share_count += restatist_data['share']
			download_count += restatist_data['download']

		db_marketanalysis.member_behavior_count.update_one(
						{'user_id': member_id},
						{'$set': {'user_id': member_id, 'resource_click_count': click_count, 'resource_share_count': share_count, 'resource_download_count': download_count}},
						upsert=True
					)


	# 找出會員點閱 分享 下載 教育大市集資源統計
	total_click = 0
	total_download = 0
	total_share = 0
	total_clickgood = 0
	total_comment = 0
	participation_datas = db_marketanalysis.member_participation.find({'member_id': member_id})
	for participation_data in participation_datas:
		total_click = participation_data['total_click']
		total_download = participation_data['total_download']
		total_share = participation_data['total_share']
		total_clickgood = participation_data['total_clickgood']
		total_comment = participation_data['total_comment']

	db_marketanalysis.member_behavior_count.update_one(
						{'user_id': member_id},
						{'$set': {'user_id': member_id, 'total_click': total_click, 'total_download': total_download,
						 'total_share': total_share, 'total_clickgood': total_clickgood, 'total_comment': total_comment,
						 'created_at': datetime.datetime.utcnow(), 'updated_at': datetime.datetime.utcnow()}},
						upsert=True
					)

cursor.close()
client.close()	
print('member_behavior_countall finished.')
