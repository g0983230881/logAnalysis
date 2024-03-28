#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_resource_usage.py
#功能說明：匯整會員最近一年的教學資源使用量
#輸入參數：無
#資料來源：marketanalysis -> search_log2
#輸出結果：marketanalysis -> member_resource_usage
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime
import sys

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]

begin_date = datetime.datetime.now()
end_date = begin_date + relativedelta(years=-1)

BehaviorType = ['F', 'M_F', 'L', 'M_L', 'R', 'M_R', 'C', 'M_C', 'MC', 'M_MC', 'SE', 'M_SE', 'SG', 'M_SG', 'SF', 'M_SF', 'ST', 'M_ST', 'SL', 'M_SL', 'H', 'M_H', 'MYRES_ARLink', 'MYRES_ARLink', 'Epedia_L', 'Evideo_L', 'Relation']
userIdlist = db_marketanalysis.identify_member.distinct('user_id')
for user_id in userIdlist :
	if user_id <= 0 :
		continue

	session_id = []
	identifyMemberSession = db_marketanalysis.identify_member.find({'user_id' : user_id})
	for identifymember in identifyMemberSession :
		session_id.append(identifymember['session_id'])


	classCodeCount = {}		
	usageCount = 0
	class_code = ''
	main_col = ''
	created_at = ''
	from_ip = ''
	userBehaviorLog = db_marketanalysis.search_log2.find({'session_id' : {'$in' : session_id}, "since": { "$gte": end_date, "$lt": begin_date}}).sort([('from_ip',1),('class_code',1), ('main_col',1), ('since',1)])
	for userlog in  userBehaviorLog :
		#同一分鐘內的相同行為不予列計
		if (from_ip!=userlog['from_ip'] and class_code!=userlog['class_code'] and main_col!=userlog['main_col'] and created_at!=unicode(userlog['since'])[0:16]) :
			from_ip = userlog['from_ip']
			class_code = userlog['class_code']
			main_col = userlog['main_col']
			created_at = unicode(userlog['since'])[0:16]

			if class_code in BehaviorType :
				if set([class_code]).issubset(classCodeCount) == True :
					classCodeCount[class_code] = classCodeCount.get(class_code) + 1
				else :
					classCodeCount[class_code] = 1
				usageCount = usageCount + 1
		else :
			continue


	db_marketanalysis.member_resource_usage.update_one(
		{'member_id': user_id},{"$set":
		{'member_id': user_id, 'total' : usageCount, 'usage' : classCodeCount, 'updated_at' : datetime.datetime.now() }},
		upsert=True
	)

