#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_active.py
#功能說明：分析每個月的酷學習/學習拍/M3教育雲呼叫API的使用量
#輸入參數：無
#資料來源：edumarket -> apilog
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

nowdate = datetime.datetime.now() + relativedelta(months=1)
nowdate = datetime.datetime(nowdate.year, nowdate.month, 1)
while (nowdate.strftime("%Y-%m") >= '2017-02') :        
    heavyUserList = db_edumarket.apilog.aggregate(
      [
         {
           '$match' : {
               'since':{'$gte':(nowdate + relativedelta(months=-1)), '$lt':nowdate},
               'member_id' : {'$in':[100622, 92234, 82198]}
           },
         },
         {
           '$group' : {
              '_id' : { 'member_id': "$member_id" },
              'count': { '$sum': 1 }
           }
         },
         { '$sort' : { 'count' : -1 } }
      ]
    )
    
    member_100622 = 0
    member_92234 = 0
    member_82198 = 0   
    for doc in list(heavyUserList) :
        if doc['_id']['member_id'] == 100622 :
            member_100622 = doc['count']
        if doc['_id']['member_id'] == 92234 :
            member_92234 = doc['count']
        if doc['_id']['member_id'] == 82198 :
            member_82198 = doc['count']

    platform_learning = {'count':member_100622}
    platform_m3 =  {'count':member_82198}
    platform_cooc =  {'count':member_92234}

    print((nowdate + relativedelta(months=-1)).strftime("%Y-%m-%d") + ' ～ ' + nowdate.strftime("%Y-%m-%d") + '  '  + str(member_100622) + ' ' + str(member_92234) + ' ' + str(member_82198))
    db_marketanalysis.learningsys_usage.update_one(
			{'year_month' : (nowdate + relativedelta(months=-1)).strftime("%Y-%m")},
			{'$set' : {'platform_learning':platform_learning, 'platform_cooc':platform_cooc, 'platform_m3':platform_m3, 'updated_at' : datetime.datetime.now()}},
			upsert=True
		)
    nowdate = nowdate + relativedelta(months=-1)


    
     


