#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_behaviorlog.py
#功能說明：匯整會員個人操作行為歷程資料
#輸入參數：無
#資料來源：edumarket -> searchlog
#輸出結果：marketanalysis -> member_behaviorlog
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
import datetime, time
import sys

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
db_edumarket = client[mongo_dbname_edumarket]
start_time = time.time()


# userIdlist = db_marketanalysis.identify_member.distinct('user_id')
# for user_id in userIdlist :
# 	session_id = []
# 	if user_id <= 0 :
# 		continue
# 	identifyMemberSession = db_marketanalysis.identify_member.find({'user_id' : user_id})
# 	for identifymember in identifyMemberSession :
# 		#print(str(identifymember['user_id']) + '   ' + identifymember['session_id'])
# 		session_id.append(identifymember['session_id'])

# 	class_code = ''
# 	main_col = ''
# 	created_at = ''
# 	count = 0
# 	userBehaviorLog = db_edumarket.searchlog.find({'session_id' : {'$in' : session_id}}).sort([('class_code',1), ('main_col',1), ('since',1)])
# 	for userlog in  userBehaviorLog :
# 		#count = count + 1
# 		#print(str(count))
# 		#同一分鐘內的相同行為不予列計
# 		if (class_code!=userlog['class_code'] or main_col!=userlog['main_col'] or created_at!=str(userlog['since'])[0:16]) :
# 			class_code = userlog['class_code']
# 			main_col = userlog['main_col']
# 			created_at = str(userlog['since'])[0:16]	

# 			db_marketanalysis.member_behaviorlog.update_one(
# 				{'_id': userlog['_id']},{"$set":
# 				{'_id': userlog['_id'], 'member_id' : user_id, 'from_ip' : userlog['from_ip'], 'eventtime' : userlog['since'], 
# 				 'class_code' : userlog['class_code'], 'main_col' : userlog['main_col'], 'sec_col' : userlog['sec_col']}},
# 				upsert=True
# 			)
# 		else :
# 			continue


## 經session_id回復找出之使用者 ##
idListDistinctStart = datetime.datetime.now()
userIdlist = db_marketanalysis.identify_member.distinct('user_id')
idListDistinctEnd = datetime.datetime.now()
print('idListDistinct_DeltaTime:		{}.'.format(idListDistinctEnd - idListDistinctStart))

for user_id in userIdlist:
	userIdlistStart = datetime.datetime.now()
	print(user_id)
	

	## 檢查該會員有無last_log, 上次有沒有跑過
	is_existsStart = datetime.datetime.now()
	is_exists = db_marketanalysis.identify_member_lastlog.find({'user_id': user_id}).explain()['executionStats']['nReturned']
	is_existsEnd = datetime.datetime.now()
	print('findSessionId_DeltaTime:		{}.'.format(is_existsEnd - is_existsStart))
	
	if (is_exists >= 1):  ## 有last_log, 從上次執行最後一筆的時間往後繼續跑
		last_datas = db_marketanalysis.identify_member_lastlog.find({'user_id': user_id})

		last_date = ''
		
		last_dataStart = datetime.datetime.now()
		for last_data in last_datas:
			last_date = last_data['last_date']
		last_dataEnd = datetime.datetime.now()
		print('last_data_loopDeltaTime:		{}.'.format(last_dataEnd - last_dataStart))


		identifyMemberSessionStart = datetime.datetime.now()
		session_id = []  ## 存放該使用者所有認證session_id
		identifyMemberSession = db_marketanalysis.identify_member.find({'user_id' : user_id})
		for identifymember in identifyMemberSession :
			#print(str(identifymember['user_id']) + '   ' + identifymember['session_id'])
			session_id.append(identifymember['session_id'])
		identifyMemberSessionEnd = datetime.datetime.now()
		print('identifyMemberSession_loopDeltaTime:	{}.'.format(identifyMemberSessionEnd - identifyMemberSessionStart))

		session_id = list(dict.fromkeys(session_id))

		findand1Start = datetime.datetime.now()
		is_exists = db_edumarket.searchlog.find({'$and': [{'session_id': {'$in': session_id}},{'since': {'$gt': last_date}}]}).explain()['executionStats']['nReturned']
		findand1End = datetime.datetime.now()
		print('findand1_DeltaTime:			{}.'.format(findand1End - findand1Start))


		if (is_exists >= 1):
			class_code = ''
			main_col = ''
			created_at = ''

			findand2Start = datetime.datetime.now()
			userBehaviorLog = db_edumarket.searchlog.find({'$and': [{'session_id': {'$in': session_id}}, {'since': {'$gt': last_date}}]})
			findand2End = datetime.datetime.now()
			print('findand2_DeltaTime:			{}.'.format(findand2End - findand2Start))
			
			userBehaviorLogStart1 = datetime.datetime.now()
			for userlog in userBehaviorLog :
				#同一分鐘內的相同行為不予列計
				if (class_code!=userlog['class_code'] or main_col!=userlog['main_col'] or created_at!=str(userlog['since'])[0:16]) :
					class_code = userlog['class_code']
					main_col = userlog['main_col']
					created_at = str(userlog['since'])[0:16]

					db_marketanalysis.member_behaviorlog.update_one(
						{'_id': userlog['_id']},{"$set":
						{'_id': userlog['_id'], 'member_id' : user_id, 'from_ip' : userlog['from_ip'], 'eventtime' : userlog['since'], 
						 'class_code' : userlog['class_code'], 'main_col' : userlog['main_col'], 'sec_col' : userlog['sec_col']}},
						upsert=True
					)

				else:
					userBehaviorLogEnd1 = datetime.datetime.now()
					print('userBehaviorLog1_continue_loopDeltaTime:	{}.'.format(userBehaviorLogEnd1 - userBehaviorLogStart1))
					continue
			userBehaviorLogEnd1 = datetime.datetime.now()
			print('userBehaviorLog1_loopDeltaTime:		{}.'.format(userBehaviorLogEnd1 - userBehaviorLogStart1))

			## 所有符合同user的session_id[]裡, 最後一筆log的時間&_id存入db 
			sessionLastLogStart1 = datetime.datetime.now()
			sessionLastLog = db_edumarket.searchlog.find({'session_id': {'$in': session_id}}).sort('since', -1).limit(1)
			for sessionLastlog in sessionLastLog:
				db_marketanalysis.identify_member_lastlog.update_one(
					{'_id': sessionLastlog['_id']},{"$set":
					{'_id': sessionLastlog['_id'], 'last_date': sessionLastlog['since'], 'user_id': user_id}},
					upsert=True
					)
			sessionLastLogEnd1 = datetime.datetime.now()
			print('sessionLastLog1_loopDeltaTime:		{}.'.format(sessionLastLogEnd1 - sessionLastLogStart1))

	## 之前無lastlog, 代表第一次跑, 該使用者需從searchlog第一筆開始比對
	else:
		session_id = []
		if user_id <= 0:
			userIdlistEnd = datetime.datetime.now()
			print('userId<=0_loopDeltaTime: 		{}.'.format(userIdlistEnd - userIdlistStart))
			continue
		if user_id == 13834:
			userIdlistEnd = datetime.datetime.now()
			print('userId==13834_loopDeltaTime: 		{}.'.format(userIdlistEnd - userIdlistStart))
			continue

		
		identifyMemberSessionStart = datetime.datetime.now()
		identifyMemberSession = db_marketanalysis.identify_member.find({'user_id' : user_id})
		for identifymember in identifyMemberSession :
			#print(str(identifymember['user_id']) + '   ' + identifymember['session_id'])
			session_id.append(identifymember['session_id'])
		identifyMemberSessionEnd = datetime.datetime.now()
		print('identifyMemberSession_loopDeltaTime:	{}.'.format(identifyMemberSessionEnd - identifyMemberSessionStart))
		session_id = list(dict.fromkeys(session_id))

		class_code = ''
		main_col = ''
		created_at = ''
		try:
			userBehaviorLogStart2 = datetime.datetime.now()
			userBehaviorLog = db_edumarket.searchlog.find({'session_id' : {'$in' : session_id}}).sort([('class_code',1), ('main_col',1), ('since',1)])
			for userlog in  userBehaviorLog :
				if (class_code!=userlog['class_code'] or main_col!=userlog['main_col'] or created_at!=str(userlog['since'])[0:16]) :
					class_code = userlog['class_code']
					main_col = userlog['main_col']
					created_at = str(userlog['since'])[0:16]

					db_marketanalysis.member_behaviorlog.update_one(
						{'_id': userlog['_id']},{"$set":
						{'_id': userlog['_id'], 'member_id' : user_id, 'from_ip' : userlog['from_ip'], 'eventtime' : userlog['since'], 
						 'class_code' : userlog['class_code'], 'main_col' : userlog['main_col'], 'sec_col' : userlog['sec_col']}},
						upsert=True
					)

				else:
					userBehaviorLogEnd2 = datetime.datetime.now()
					continue
			userBehaviorLogEnd2 = datetime.datetime.now()
			print('userBehaviorLog2_loopDeltaTime:		{}.'.format(userBehaviorLogEnd2 - userBehaviorLogStart2))
			
		## 如session_id[] 過大 超過記憶體負荷, 就一筆一筆session_id 去抓取符合的log
		except:
			session_idStart = datetime.datetime.now()
			for sessionid in session_id:
				userBehaviorLog = db_edumarket.searchlog.find({'session_id' : sessionid}).sort([('class_code',1), ('main_col',1), ('since',1)])
				for userlog in  userBehaviorLog :
					if (class_code!=userlog['class_code'] or main_col!=userlog['main_col'] or created_at!=str(userlog['since'])[0:16]) :
						class_code = userlog['class_code']
						main_col = userlog['main_col']
						created_at = str(userlog['since'])[0:16]

						db_marketanalysis.member_behaviorlog.update_one(
							{'_id': userlog['_id']},{"$set":
							{'_id': userlog['_id'], 'member_id' : user_id, 'from_ip' : userlog['from_ip'], 'eventtime' : userlog['since'], 
							 'class_code' : userlog['class_code'], 'main_col' : userlog['main_col'], 'sec_col' : userlog['sec_col']}},
							upsert=True
						)
					else:
						session_idEnd = datetime.datetime.now()
						print('session_id_loopDeltaTime:		{}.'.format(session_idEnd - session_idStart))
						continue
			session_idEnd = datetime.datetime.now()
			print('session_id_loopDeltaTime:		{}.'.format(session_idEnd - session_idStart))


		## 所有符合同user的session_id[]裡, 最後一筆log的時間&_id存入db 
		sessionLastLog = db_edumarket.searchlog.find({'session_id' : {'$in' : session_id}}).sort('since', -1).limit(1)

		sessionLastLogStart2 = datetime.datetime.now()
		for sessionLastlog in sessionLastLog:
			db_marketanalysis.identify_member_lastlog.update_one(
				{'_id': sessionLastlog['_id']},{"$set":
				{'_id': sessionLastlog['_id'], 'last_date': sessionLastlog['since'], 'user_id': user_id}},
				upsert=True
			)
		sessionLastLogEnd2 = datetime.datetime.now()
		print('sessionLastLog2_loopDeltaTime:		{}.'.format(sessionLastLogEnd2 - sessionLastLogStart2))

	userIdlistEnd = datetime.datetime.now()
	print('userIdlist_loopDeltaTime: 		{}.'.format(userIdlistEnd - userIdlistStart))

print("--- ran %s seconds ---" % (time.time() - start_time))
