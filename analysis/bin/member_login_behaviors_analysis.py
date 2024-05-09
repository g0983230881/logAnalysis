#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：ayear_member_active.py
#功能說明：# 分析活躍會員人數 一個年份, 一個月份 登入3次以上
	  	   # 分析一個月份 未登入使用者	
	  	   # 最近一年內 有登入的會員人次	
#輸入參數：無
#資料來源：# edumarket -> educloud_loginhistory, apilog, resources_usage_tpcooc
		   # marketanalysis -> member_growing_all
#輸出結果：insert to marketanalysis -> working_performance
#開發人員：Shih-Chun Hwang

from pymongo import MongoClient
import datetime
import sys

sys.path.append("..")
from settings import *

client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]

nowdate = datetime.datetime.now()
year = nowdate.year
month = nowdate.month
day = nowdate.day

ayear_from_dt = nowdate - datetime.timedelta(days=365)   # 一年前
now_dt = datetime.datetime(year, month, day, 23, 59, 59) # 今天
FirstDate_109Year = datetime.datetime(2020, 1, 1)        # 109年1月1日
FirstDate_ThisMonth = datetime.datetime(year, month, 1)  # 這個月的第一天
FirstDate_NextMonth = FirstDate_ThisMonth                # 下個月的第一天
if month == 12 :
	FirstDate_NextMonth = datetime.datetime(year+1, 1, 1) 
else:
	FirstDate_NextMonth = datetime.datetime(year, month+1, 1)

 

year_month = ''             # 取得當月年份及月份的字串
if month < 10:
	year_month = str(year)+'-0'+str(month)
else:
	year_month = str(year)+'-'+str(month)

# 計算活躍性會員人數－最近一年內有登入３次以上的會員
heavyUserCount = 0
heavyUserList = db_edumarket.educloud_loginhistory.aggregate(
   [
      {
        '$match' : {
           'created_at':{'$gte': ayear_from_dt, '$lt': now_dt}
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
for doc in list(heavyUserList) :
	if (doc['count'] > 3) :
		heavyUserCount= heavyUserCount+1; 


# 計算本月未登入的使用者數量
monthUnloginCount = 0
month_unLogin = db_marketanalysis.search_log2.aggregate(
    [
       {
         '$match' : {
            'user_id':{'$lte': 0},
            'since':{'$gte': FirstDate_ThisMonth, '$lt': FirstDate_NextMonth}
         },
       },
       {
         '$group' : {
            '_id' : { 'UserId': '$session_id' },
            'count': { '$sum': 1 }
         }
       },
       { '$sort' : { 'count' : -1 } },
    ],allowDiskUse=True
)
monthUnloginCount = len(list(month_unLogin))

#統計資源點閱,下載,分享,點讚等數量
total_restatist = db_edumarket.restatist.aggregate([
   {
     '$group': {
       '_id' : -1,
       'click': { '$sum': '$click'},
       'url_click': { '$sum': '$url_click'},
       'good': { '$sum': '$good'},
       'download': { '$sum': '$download'},
       'share': { '$sum': '$share'}
     }
   }
])

restatist_result = list(total_restatist)
resources_click = restatist_result[0]['click']
resources_download = restatist_result[0]['url_click'] + restatist_result[0]['download']
resources_good = restatist_result[0]['good']
resources_share = restatist_result[0]['share']


# 計算最近一年的登入會員人數
yearLoginCount = 0
logindatas = db_edumarket.educloud_loginhistory.distinct('UserId', {'created_at': {'$gte': ayear_from_dt ,'$lt': now_dt}});
yearLoginCount = len(logindatas)

# 計算本月份的會員登入率
monthLoginCount = 0 # 本月份的會員登入人數
logindatas = db_edumarket.educloud_loginhistory.distinct('UserId', {'created_at': {'$gte': FirstDate_ThisMonth ,'$lt': FirstDate_NextMonth}});
monthLoginCount = len(logindatas)

# 取得會員總數
member_amount = 0 
growing_datas = db_marketanalysis.member_growing_all.find({'year_month': year_month});
first_record = next(growing_datas, None) #先判斷有沒有值
if first_record is not None:
    member_amount = first_record.get('total', 0)

try:
	monthLoginRate = float(monthLoginCount) / float(member_amount)  
except:
	monthLoginRate = 0.0 # 登入率


# 計算109年起至本月份的OpenAPI使用次數
APIcount = db_edumarket.apilog.count_documents({'since' : {'$gte': FirstDate_109Year, '$lt': FirstDate_NextMonth}}) + db_edumarket.resources_usage_tpcooc.count_documents({'created_at': {'$gt': FirstDate_109Year, '$lt': FirstDate_NextMonth}})
print(APIcount)

# 計算自109年起累計至本月之點閱數
resources_click_begin109Year = db_marketanalysis.search_log2.count_documents({'class_code':'R', "since" : {'$gte': FirstDate_109Year, '$lt':FirstDate_NextMonth}})
# 計算自109年起累計至本月之下載數
resources_download_begin109Year = db_marketanalysis.search_log2.count_documents({'class_code':{'$in' : ['F','L']}, "since" : {'$gte' : FirstDate_109Year, '$lt':FirstDate_NextMonth}})
# 計算自109年起累計至本月之點讚數
resources_good_begin109Year = db_marketanalysis.search_log2.count_documents({'class_code':'MC', "since" : {'$gte' : FirstDate_109Year, '$lt':FirstDate_NextMonth}})
# 計算自109年起累計至本月之分享數
resources_share_begin109Year = db_marketanalysis.search_log2.count_documents({'class_code':{'$in' : ['SE','SG','SF','ST','SL']}, "since" : {'$gte' : FirstDate_109Year, '$lt':FirstDate_NextMonth}})
print(resources_click_begin109Year, resources_download_begin109Year, resources_good_begin109Year, resources_share_begin109Year)



db_marketanalysis.working_performance.update_one(
		{'year_month': year_month},
		{'$set' : {'created_at': nowdate, 'year_month': year_month, 'heavy_user': heavyUserCount, 'unlogin_user': monthUnloginCount, 'year_did_login': yearLoginCount, 'month_did_login': monthLoginCount, 'month_login_rate': monthLoginRate, 'open_API_used': APIcount, 'member_total': member_amount, 'resources_click': resources_click, 'resources_good':resources_good, 'resources_share':resources_share, 'resources_download':resources_download, 'resources_click_begin109Year':resources_click_begin109Year, 'resources_download_begin109Year':resources_download_begin109Year, 'resources_good_begin109Year':resources_good_begin109Year, 'resources_share_begin109Year':resources_share_begin109Year }},
		upsert = True
	)

client.close()
print('member_login_behavior_analysis finished.')