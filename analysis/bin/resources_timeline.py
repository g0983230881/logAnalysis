#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：resources_timeline.py
#功能說明：分析資源使用的時序性關係
#輸入參數：無
#資料來源：marketanalysis -> search_log2
#輸出結果：
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime
import sys

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]

nowdate = datetime.datetime.now()
lastMonthDate = nowdate + relativedelta(days=-30)

i = 0
userList = db_marketanalysis.search_log2.distinct("user_id", {"since" : {"$gte" : lastMonthDate}, 'user_id' : {'$gt': 0}})
print('userList distinct finished.\nuserList: ', userList)

for user_id in userList :
     recordCount = db_marketanalysis.search_log2.count_documents({'class_code':'R', 'user_id': user_id, 'since':{'$gte' : lastMonthDate}})     
     print('recordCount: ', recordCount)

     if (recordCount > 1) :
         i = i + 1
         last_resources_id = ""
         last_session_id = ""
         resourcesData = db_marketanalysis.search_log2.find({'class_code':'R', 'user_id': user_id, 'since':{'$gte' : lastMonthDate}})
         for resources in resourcesData :
             if (last_resources_id == "" or last_session_id != resources['session_id']) :
                 last_resources_id =  resources['main_col']
                 last_session_id = resources['session_id']
                 print(resources['main_col'])
             else :
                 if (last_resources_id != resources['main_col']) :
                     print(last_resources_id + " -> " + resources['main_col'])
                     
                     try :
                         if (int(last_resources_id) <= 0 or int(resources['main_col']) <= 0) :
                             continue
                     except :
                         continue

                     db_marketanalysis.resources_timeline.update_one(
                         {'user_id' : user_id, 'current' : int(last_resources_id), 'next': int(resources['main_col'])},
                            {'$set' : {'user_id' : user_id, 'current' : int(last_resources_id), 'next': int(resources['main_col']), 
                                                    'eventdate':resources['since'].strftime("%Y-%m-%d"), 'updated_at' : datetime.datetime.now()}},
                            upsert=True
                            )
                 last_session_id = resources['session_id']
                 last_resources_id = resources['main_col']
             print(resources['since'].strftime("%Y-%m-%d %H:%M:%S") + '  ' + str(resources['user_id']) + '  ' +  resources['main_col'] + '  ' + resources['sec_col'])

         #if i > 5 :
         #   break;

client.close()
print('resources_timeline finished.')