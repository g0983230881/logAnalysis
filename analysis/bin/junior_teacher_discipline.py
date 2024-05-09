#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：junior_teacher_discipline.py
#功能說明：從使用者的行為記錄中分析老師下載、收藏、追蹤過的資源，以此分析出可能的授課科目，以作為改善國中老師資源推薦的參考依據
#輸入參數：
#資料來源：mongodb -> searchlog, mysql -> member, resources
#輸出結果：marketanalysis -> teacher_discipline
#開發人員：Derek

from pymongo import MongoClient
from importlib import reload
import sys,json,operator
import pymysql
import datetime

sys.path.append("..")
from settings import *

reload(sys)

client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
conn.autocommit = True
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor3 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor4 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor5 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor6 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor7 = conn.cursor(cursor=pymysql.cursors.DictCursor)

roleSQL = 'select * from member where status="N" and id={0}'
res_objSQL = 'select * from resources_objfile where isdelete="N" and objectfile_id={0}'
resources_kmSQL = 'select * from resources_km where isdelete="N" and type="level" and resources_id=%s'
domainSQL = 'select * from domain where isdelete="N" and resources_id={0}'
disciplineSQL = 'select * from discipline where id={0}'
kmlevel1SQL = 'select * from KM_Level1 where isdelete="N" and id={0}'
kmlevel2SQL = 'select * from KM_Level2 where isdelete="N" and id={0}'
km_disciplineSQL = 'select * from KM_discipline where isdelete="N" and id={0}'


## 取得近一年內活躍使用者編號陣列
user_id_listStartTime = datetime.datetime.now()
user_id_list = db_edumarket.searchlog.find({'class_code': {'$in': ['F', 'M_F', 'Resource_Follow']}}).distinct('user_id')
print('user_id_list deltatime: ', datetime.datetime.now() - user_id_listStartTime)

user_id_list.remove(-1)
# user_id_list = [148547, 148554, 148559, 148561, 126525, 148565, 130083, 148568, 148569, 148576, 148580, 90964, 148581, 148583, 148584, 148589, 148593, 148602, 148606, 148609, 148610, 148611, 148613, 148617, 148619, 148620, 148621, 148642, 84078, 148667, 148670, 148676, 148677, 148668, 148680, 148679, 148683, 148682, 148681, 148690, 82777, 148698, 148702, 124965, 133922, 148713, 148715, 148721, 92730, 148748, 148756, 148758, 131435, 148763, 148772, 148771, 90650, 148775, 148784, 148786, 148787, 146886, 131632, 148797, 148804, 125722, 148809, 148812, 148811, 148814, 148816, 148821, 148826, 148828, 148832, 136776, 122063, 81183, 148845, 148850, 133796, 148856, 148848]
# user_id_list = [137457]

for user_id in user_id_list:
	roletype = ''
	edutype = ''
	school_id = ''
	user_dis_arr = {}

	# 取得各會員的身分
	SQL = 'select * from member where status="N" and id=' + str(user_id)
	cursor.execute(roleSQL.format(user_id))
	memberRow = cursor.fetchone()
	if memberRow is not None:	
		roletype = memberRow.get('roletype');
		edutype = memberRow.get('edutype');
		school_id = memberRow.get('school_id');

	usage_datas = db_edumarket.searchlog.find({'user_id': user_id, 'class_code': {'$in': ['F', 'M_F', 'Resource_Follow']}})
	for usage_data in usage_datas:
		class_code = usage_data['class_code']
		res_id = usage_data['main_col']

		## 下載的資源編號不同, 需從resources_objfile找回資源編號
		if (res_id != '' and class_code == 'F' or class_code == 'M_F'):
			cursor2.execute(res_objSQL.format(res_id))
			res_objRow = cursor2.fetchone()
			if res_objRow is not None:	
				res_id = str(res_objRow.get('resources_id'))

		if (res_id != '' and res_id.isdigit()):
			# ## 查詢被使用資源的 資源領域、階段
			rows_count = cursor3.execute(resources_kmSQL, int(res_id))
			## resources_km 沒有該筆資源的 領域、階段資訊, 改由domain資料表內查詢disciplin_id
			if (rows_count == 0):  
				cursor4.execute(domainSQL.format(res_id))
				res_km_data = cursor4.fetchone()
				if res_km_data is not None:
					dis_id = res_km_data.get('discipline_id')
					cursor5.execute(disciplineSQL.format(dis_id))
					dis_data = cursor5.fetchone()
					if dis_data is not None:
						level_name = dis_data.get('name')
						if level_name in user_dis_arr.keys():
							user_dis_arr[level_name] += 1
						else:
							user_dis_arr[level_name] = 1
				else:
					continue

			## resources_km 有資料, 就解析學習領域階段
			elif (rows_count > 0):
				res_km_datas = cursor3.fetchall()
				for res_km_data in res_km_datas:
					level_name = ''
					km_content = res_km_data.get('content')
					content_list = km_content.split(',')

					if (len(content_list) == 1):  ## 只有紀錄學科
						level1_list = content_list[0].split('-');
						if (len(level1_list) == 1 and level1_list[0] != ''):  ## 舊版課綱
							cursor4.execute(kmlevel1SQL.format(level1_list[0]))
							level_name = cursor4.fetchone().get('name')
							if level_name in user_dis_arr.keys():
								user_dis_arr[level_name] += 1
							else:
								user_dis_arr[level_name] = 1

						elif (len(level1_list) > 1 and level1_list[0] != '' and level1_list[1] != ''):  ## 新版課綱
							cursor4.execute(kmlevel1SQL.format(level1_list[0]))
							level1_Fname = cursor4.fetchone().get('name')

							cursor7.execute(km_disciplineSQL.format(level1_list[1]))
							level1_Sname = cursor7.fetchone().get('name')

							if level1_Fname == level1_Sname:
								level_name = level1_Fname
							else:
								level_name = level1_Fname + '-' + level1_Sname

							if level_name in user_dis_arr.keys():
								user_dis_arr[level_name] += 1
							else:
								user_dis_arr[level_name] = 1
					
	if (len(user_dis_arr) > 0):
		top5_count = 0
		user_dis_list = sorted(user_dis_arr.items(), key=operator.itemgetter(1), reverse=True)
		user_dis_arr = {}
		for user_dis in user_dis_list:
			if top5_count < 5:
				user_dis_arr[user_dis[0]] = user_dis[1]
				top5_count += 1
			else:
				break

		print('user_id: ' + str(user_id))
		print(json.dumps(user_dis_arr, ensure_ascii=False))
		db_marketanalysis.teacher_discipline.update_one(
			{'user_id' : user_id, 'roletype': roletype, 'edutype': edutype, 'school_id': school_id},
			{'$set' : {'user_id' : user_id, 'roletype': roletype, 'edutype': edutype, 'school_id': school_id, 'discipline_used': user_dis_arr}},
			upsert=True
		)

conn.close()
client.close()
print('junior_teacher_discipline finished.')