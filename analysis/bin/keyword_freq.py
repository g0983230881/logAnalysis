#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：keyword_freq.py
#功能說明：分析每個月的全文檢索詞出現頻率,及出現前20名的詞
#輸入參數：無
#資料來源：
#輸出結果：
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
import datetime
import sys

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]

#分析每個月的全文檢索詞出現頻率
nowdate = datetime.datetime.now()
beforeOneYearDate = datetime.datetime(nowdate.year, nowdate.month, 1)

keywordFreqList = db_marketanalysis.search_log2.aggregate(
   [
      {
        '$match' : {
           'since':{'$gte':beforeOneYearDate, '$lt':nowdate},
           'class_code': {'$in': ['Q','QSK']}
        },
      },
      {
        '$group' : {
           '_id' : { 'keyword' : "$sec_col" },
           'count': { '$sum': 1 }
        }
      },
      { '$sort' : { 'count' : -1 } }
   ]
)
topKeyword = {}
i=1
totalCount = 0
for doc in list(keywordFreqList) :
    totalCount= totalCount + doc['count']  
    if i <= 20 : 
        key = doc['_id']['keyword']
        topKeyword[key] = doc['count']
    i = i + 1

print(beforeOneYearDate.strftime("%Y-%m-%d") + ' --- ' + nowdate.strftime("%Y-%m-%d") + '  ' + str(totalCount))
db_marketanalysis.keyword_freq.update_one(
			{'year_month' : nowdate.strftime("%Y-%m")},
			{'$set' : {'year_month' : nowdate.strftime("%Y-%m"), 'total' : totalCount, 'top20keyword' : topKeyword, 'updated_at' : datetime.datetime.now()}},
			upsert=True
		)


nowdate = datetime.datetime(nowdate.year, nowdate.month, 1)
while (nowdate.strftime("%Y-%m") >= '2018-01') :
    keywordFreqList = db_marketanalysis.search_log2.aggregate(
      [
         {
           '$match' : {
               'since':{'$gte':(nowdate + relativedelta(months=-1)), '$lt':nowdate},
               'class_code': {'$in': ['Q','QSK']}
           },
         },
         {
           '$group' : {
              '_id' : { 'keyword' : "$sec_col" },
              'count': { '$sum': 1 }
           }
         },
         { '$sort' : { 'count' : -1 } }
      ]
    )
    topKeyword = {}
    i=1
    totalCount = 0
    for doc in list(keywordFreqList) :
        totalCount= totalCount + doc['count']
        if i <= 20 : 
            key = doc['_id']['keyword']
            topKeyword[key] = doc['count']
        i = i + 1 


    print((nowdate + relativedelta(months=-1)).strftime("%Y-%m-%d") + ' --- ' + nowdate.strftime("%Y-%m-%d") + ' -> ' + '  ' + str(totalCount))
    db_marketanalysis.keyword_freq.update_one(
			{'year_month' : (nowdate + relativedelta(months=-1)).strftime("%Y-%m")},
			{'$set' : {'year_month' : nowdate.strftime("%Y-%m"), 'total' : totalCount, 'top20keyword' : topKeyword, 'updated_at' : datetime.datetime.now()}},
			upsert=True
		)
    nowdate = nowdate + relativedelta(months=-1)



client.close()

