#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_active.py
#功能說明：分析每個月的活動會員及活躍性會員數
#輸入參數：無
#資料來源：edumarket -> educloud_loginhistory
#輸出結果：
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from os.path import dirname
from dateutil.relativedelta import relativedelta
import datetime
import sys

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]

nowdate = datetime.datetime.now()
beforeOneYearDate = nowdate + relativedelta(years=-1)


#活動會員統計
activeUserList = db_edumarket.educloud_loginhistory.distinct("UserId", {"created_at" : {"$gte":beforeOneYearDate,"$lt":nowdate}})
#活躍性會員統計
heavyUserList = db_edumarket.educloud_loginhistory.aggregate(
   [
      {
        '$match' : {
           'created_at':{'$gte':beforeOneYearDate, '$lt':nowdate}
        },
      },
      {
        '$group' : {
           '_id' : { 'UserId': "$UserAccount" },
           'count': { '$sum': 1 }
        }
      },
      { '$sort' : { 'count' : -1 } }
   ]
)
totalCount = 0;
for doc in list(heavyUserList) :
     if (doc['count'] > 3) :
         totalCount= totalCount+1; 

print(beforeOneYearDate.strftime("%Y-%m-%d") + ' --- ' + nowdate.strftime("%Y-%m-%d") + '  ' + str(len(activeUserList))   + '  ' + str(totalCount))


nowdate = datetime.datetime(nowdate.year, nowdate.month, 1)
while (nowdate.strftime("%Y-%m") >= '2017-02') :        
    #活動會員統計
    activeUserList = db_edumarket.educloud_loginhistory.distinct("UserId", {"created_at" : {"$gte":(nowdate + relativedelta(years=-1)),"$lt":nowdate}})
    #活躍性會員統計
    heavyUserList = db_edumarket.educloud_loginhistory.aggregate(
      [
         {
           '$match' : {
               'created_at':{'$gte':(nowdate + relativedelta(years=-1)), '$lt':nowdate}
           },
         },
         {
           '$group' : {
              '_id' : { 'UserId': "$UserAccount" },
              'count': { '$sum': 1 }
           }
         },
         { '$sort' : { 'count' : -1 } }
      ]
    )
    totalCount = 0;
    for doc in list(heavyUserList) :
        if (doc['count'] > 3) :
            totalCount= totalCount+1;    

    print((nowdate + relativedelta(years=-1)).strftime("%Y-%m-%d") + ' --- ' + nowdate.strftime("%Y-%m-%d") + ' -> ' + str(len(activeUserList))   + '  ' + str(totalCount))
    nowdate = nowdate + relativedelta(months=-1)


    
     


