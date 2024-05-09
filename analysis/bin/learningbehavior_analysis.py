#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：member_behavior_analysis.py
#功能說明：依照不同屬性，進行學習行為分析
#輸入參數：無
#資料來源：marketanalysis -> search_log2
#輸出結果：marketanalysis -> member_behavior_count

from pymongo import MongoClient
from os.path import dirname
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import sys, pymysql

sys.path.append("..")
from settings import *


client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)


lastLog_id = ''
ini_start_dt = ''
ini_end_dt = ''

is_lastlog_exists = db_marketanalysis.lastlog.find({'type': 'learning_behavior_analysis'}).explain()['executionStats']['nReturned']  ## 是否存在lastId
if is_lastlog_exists == 1:
    lastLog_data = db_marketanalysis.lastlog.find({'type': 'learning_behavior_analysis'})
    for lastLog in lastLog_data:
        lastLog_id = lastLog['id']

    is_last_datas_exists = db_marketanalysis.search_log2.find({'_id': lastLog_id}).explain()['executionStats']['nReturned']
    if is_last_datas_exists == 1:
        last_datas = db_marketanalysis.search_log2.find({'_id': lastLog_id})
        for last_data in last_datas:
            ini_start_dt = last_data['since']
    else:
        yearMonth_data = db_marketanalysis.search_log2.find().sort('since', 1).limit(1);    
        for yearMonth in yearMonth_data:
            ini_start_dt = yearMonth['since']
    
    is_end_date_data_exists = db_marketanalysis.search_log2.find().sort('since', -1).limit(1).explain()['executionStats']['nReturned']
    if is_end_date_data_exists == 1:
        end_date_data = db_marketanalysis.search_log2.find().sort('since', -1).limit(1);
        for end_date in end_date_data:
            lastLog_id = end_date['_id']
            ini_end_dt = end_date['since']
    else:
        end_date_data = db_marketanalysis.search_log2.find().sort('since', -1).limit(1);
        for end_date in end_date_data:
            lastLog_id = end_date['_id']
            ini_end_dt = end_date['since']
else:
    ## 找出search_log2中開始與結束時間
    yearMonth_data = db_marketanalysis.search_log2.find().sort('since', 1).limit(1);    
    for yearMonth in yearMonth_data:
        ini_start_dt = yearMonth['since']
    end_date_data = db_marketanalysis.search_log2.find().sort('since', -1).limit(1);
    for end_date in end_date_data:
        lastLog_id = end_date['_id']
        ini_end_dt = end_date['since']

start_year = ini_start_dt.year
start_month = ini_start_dt.month
end_year = ini_end_dt.year
end_month = ini_end_dt.month

start_dt = datetime.strptime(str(start_year) + '-' + str(start_month), '%Y-%m')
end_dt = datetime.strptime(str(end_year) + '-' + str(end_month), '%Y-%m') + relativedelta(months=+1)  


## 找出男女學童的user_id
maleId_list = []  ## 存放男學童id
femaleId_list = []  ## 存放女學童id
SQL = 'select * from member where roletype="03" and status="N"'
cursor.execute(SQL)
member_datas = cursor.fetchall()
for member_data in member_datas:
    if (member_data.get('gender') == 'M'):
        maleId_list.append(member_data.get('id'))
    if (member_data.get('gender') == 'F'):
        femaleId_list.append(member_data.get('id'))    
## -----------------------------------------------        


## 找出屬於各學習階段的資源
level1_list = []
level2_list = []
level3_list = []
level4_list = []
level5_list = []
SQL1 = 'select * from KM_Level2 where isdelete="N"'
cursor.execute(SQL1)
level_datas = cursor.fetchall()
# str 轉 unicode -> .decode('utf-8'), unicode 轉 str -> .encode('utf-8')
for level_data in level_datas:
    if (level_data.get('name') == '一'):
        level1_list.append(level_data.get('id'))
    if (level_data.get('name') == '二'):
        level2_list.append(level_data.get('id'))
    if (level_data.get('name') == '三'):
        level3_list.append(level_data.get('id'))
    if (level_data.get('name') == '四'):
        level4_list.append(level_data.get('id'))
    if (level_data.get('name') == '五'):
        level5_list.append(level_data.get('id'))

level1_resid_list = []
SQL2 = 'select * from resources_km where '
for level1_id in level1_list:
    SQL2 += 'content like "%,' + str(level1_id) + ',%"' 
    if (level1_list[-1] != level1_id):
        SQL2 += ' or '

cursor.execute(SQL2) 
level1Res_datas = cursor.fetchall()  
for level1Res_data in level1Res_datas:
    level1_resid_list.append(str(level1Res_data.get('resources_id')));

level2_resid_list = []
SQL2 = 'select * from resources_km where '
for level2_id in level2_list:
    SQL2 += 'content like "%,' + str(level2_id) + ',%"' 
    if (level2_list[-1] != level2_id):
        SQL2 += ' or '
cursor.execute(SQL2) 
level2Res_datas = cursor.fetchall()  
for level2Res_data in level2Res_datas:
    level2_resid_list.append(str(level2Res_data.get('resources_id')));    

level3_resid_list = []
SQL2 = 'select * from resources_km where '
for level3_id in level3_list:
    SQL2 += 'content like "%,' + str(level3_id) + ',%"' 
    if (level3_list[-1] != level3_id):
        SQL2 += ' or '
cursor.execute(SQL2) 
level3Res_datas = cursor.fetchall()  
for level3Res_data in level3Res_datas:
    level3_resid_list.append(str(level3Res_data.get('resources_id')));     

level4_resid_list = []
SQL2 = 'select * from resources_km where '
for level4_id in level4_list:
    SQL2 += 'content like "%,' + str(level4_id) + ',%"' 
    if (level4_list[-1] != level4_id):
        SQL2 += ' or '
cursor.execute(SQL2) 
level4Res_datas = cursor.fetchall()  
for level4Res_data in level4Res_datas:
    level4_resid_list.append(str(level4Res_data.get('resources_id')));

level5_resid_list = []
SQL2 = 'select * from resources_km where '
for level5_id in level5_list:
    SQL2 += 'content like "%,' + str(level5_id) + ',%"' 
    if (level5_list[-1] != level5_id):
        SQL2 += ' or '
cursor.execute(SQL2) 
level5Res_datas = cursor.fetchall()  
for level5Res_data in level5Res_datas:
    level5_resid_list.append(str(level5Res_data.get('resources_id')));
## -----------------------------------------------


## 找出各地理區域的使用者編號
north_list = [1, 2, 3, 4, 22, 17, 18]
center_list = [5, 6, 7, 8, 9, 19]
south_list = [10, 11, 12, 13, 20, 21, 23]
east_list = [14, 15]
out_list = [16, 24, 25]

north_member_list = []       
SQL3 = 'select * from member where status="N" and city_id in('
for north_city in north_list:
    SQL3 += str(north_city)
    if (north_list[-1] != north_city):
        SQL3 += ','
    else:
        SQL3 += ')'
cursor.execute(SQL3)
north_member_datas = cursor.fetchall()
for north_member_data in north_member_datas:
    north_member_list.append(north_member_data.get('id')) 

center_member_list = []       
SQL3 = 'select * from member where status="N" and city_id in('
for center_city in center_list:
    SQL3 += str(center_city)
    if (center_list[-1] != center_city):
        SQL3 += ','
    else:
        SQL3 += ')'
cursor.execute(SQL3)
center_member_datas = cursor.fetchall()
for center_member_data in center_member_datas:
    center_member_list.append(center_member_data.get('id'))

south_member_list = []       
SQL3 = 'select * from member where status="N" and city_id in('
for south_city in south_list:
    SQL3 += str(south_city)
    if (south_list[-1] != south_city):
        SQL3 += ','
    else:
        SQL3 += ')'
cursor.execute(SQL3)
south_member_datas = cursor.fetchall()
for south_member_data in south_member_datas:
    south_member_list.append(south_member_data.get('id'))     

east_member_list = []       
SQL3 = 'select * from member where status="N" and city_id in('
for east_city in east_list:
    SQL3 += str(east_city)
    if (east_list[-1] != east_city):
        SQL3 += ','
    else:
        SQL3 += ')'
cursor.execute(SQL3)
east_member_datas = cursor.fetchall()
for east_member_data in east_member_datas:
    east_member_list.append(east_member_data.get('id')) 

out_member_list = []       
SQL3 = 'select * from member where status="N" and city_id in('
for out_city in out_list:
    SQL3 += str(out_city)
    if (out_list[-1] != out_city):
        SQL3 += ','
    else:
        SQL3 += ')'
cursor.execute(SQL3)
out_member_datas = cursor.fetchall()
for out_member_data in out_member_datas:
    out_member_list.append(out_member_data.get('id'))     
## -----------------------------------------------

while start_dt < end_dt:
    this_startDt = start_dt + timedelta(hours=-8)
    this_endDt = start_dt + relativedelta(months=+1) + timedelta(hours=-8)
    right_endDt = start_dt + relativedelta(months=+1)
    print(str(this_startDt) + ' - ' + str(this_endDt))
    yearMonth = str(this_startDt.year) + '-' + str(this_startDt.month);

    ## 分析各區域資源使用量
    north_count = db_marketanalysis.search_log2.count_documents({'user_id': {'$in': north_member_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    center_count = db_marketanalysis.search_log2.count_documents({'user_id': {'$in': center_member_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    south_count = db_marketanalysis.search_log2.count_documents({'user_id': {'$in': south_member_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    east_count = db_marketanalysis.search_log2.count_documents({'user_id': {'$in': east_member_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    out_count = db_marketanalysis.search_log2.count_documents({'user_id': {'$in': out_member_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    
    db_marketanalysis.learning_behavior_by_region.update_one(
        {'region' : '地理區域(北)', 'year_month': yearMonth},
        {'$set' : {'region' : '地理區域(北)', 'year_month': yearMonth, 'region_count': north_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_region.update_one(
        {'region' : '地理區域(中)', 'year_month': yearMonth},
        {'$set' : {'region' : '地理區域(中)', 'year_month': yearMonth, 'region_count': center_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_region.update_one(
        {'region' : '地理區域(南)', 'year_month': yearMonth},
        {'$set' : {'region' : '地理區域(南)', 'year_month': yearMonth, 'region_count': south_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_region.update_one(
        {'region' : '地理區域(東)', 'year_month': yearMonth},
        {'$set' : {'region' : '地理區域(東)', 'year_month': yearMonth, 'region_count': east_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_region.update_one(
        {'region' : '地理區域(離島)', 'year_month': yearMonth},
        {'$set' : {'region' : '地理區域(離島)', 'year_month': yearMonth, 'region_count': out_count}},
        upsert=True
    )


    ## 分析各學習階段資源使用量
    level1_res_count = db_marketanalysis.search_log2.count_documents({'main_col': {'$in': level1_resid_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    level2_res_count = db_marketanalysis.search_log2.count_documents({'main_col': {'$in': level2_resid_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    level3_res_count = db_marketanalysis.search_log2.count_documents({'main_col': {'$in': level3_resid_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    level4_res_count = db_marketanalysis.search_log2.count_documents({'main_col': {'$in': level4_resid_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    level5_res_count = db_marketanalysis.search_log2.count_documents({'main_col': {'$in': level5_resid_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})

    db_marketanalysis.learning_behavior_by_level.update_one(
        {'level' : '一', 'year_month': yearMonth},
        {'$set' : {'level' : '一', 'year_month': yearMonth, 'resources_count': level1_res_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_level.update_one(
        {'level' : '二', 'year_month': yearMonth},
        {'$set' : {'level' : '二', 'year_month': yearMonth, 'resources_count': level2_res_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_level.update_one(
        {'level' : '三', 'year_month': yearMonth},
        {'$set' : {'level' : '三', 'year_month': yearMonth, 'resources_count': level3_res_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_level.update_one(
        {'level' : '四', 'year_month': yearMonth},
        {'$set' : {'level' : '四', 'year_month': yearMonth, 'resources_count': level4_res_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_level.update_one(
        {'level' : '五', 'year_month': yearMonth},
        {'$set' : {'level' : '五', 'year_month': yearMonth, 'resources_count': level5_res_count}},
        upsert=True
    )

    
    ## 男女學童使用資源比例
    male_count = db_marketanalysis.search_log2.count_documents({'user_id': {'$in': maleId_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    female_count = db_marketanalysis.search_log2.count_documents({'user_id': {'$in': femaleId_list}, 'since':{'$gte': this_startDt, '$lte': this_endDt}, 'class_code': {'$in': ['R', 'F']}})
    
    db_marketanalysis.learning_behavior_by_sgender.update_one(
        {'gender' : 'male', 'year_month': yearMonth},
        {'$set' : {'gender' : 'male', 'year_month': yearMonth, 'behavior_count': male_count}},
        upsert=True
    )
    db_marketanalysis.learning_behavior_by_sgender.update_one(
        {'gender' : 'female', 'year_month': yearMonth},
        {'$set' : {'gender' : 'female', 'year_month': yearMonth, 'behavior_count': female_count}},
        upsert=True
    )
    start_dt = right_endDt 


start_dt = datetime.strptime(str(start_year) + '-' + str(start_month), '%Y-%m')
end_dt = datetime.strptime(str(end_year) + '-' + str(end_month), '%Y-%m') + relativedelta(months=+1) 
## 時間重新開始 , 資料表重新命名

clock_02_count = 0
clock_04_count = 0
clock_06_count = 0
clock_08_count = 0
clock_10_count = 0
clock_12_count = 0
clock_14_count = 0
clock_16_count = 0
clock_18_count = 0
clock_20_count = 0
clock_22_count = 0
clock_24_count = 0
hour_month = start_dt.month 

while start_dt < end_dt:
    this_startDt = start_dt
    this_endDt = start_dt + relativedelta(days=+1)
    print(str(this_startDt) + ' - ' + str(this_endDt))

    this_start_hour = this_startDt  + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=2) + timedelta(hours=-8)
    clock_02_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})
    print('clock_02_count:' + str(clock_02_count))

    this_start_hour = this_startDt + timedelta(hours=2) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=4) + timedelta(hours=-8)
    clock_04_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})
    print('clock_04_count:' + str(clock_04_count))

    this_start_hour = this_startDt + timedelta(hours=4) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=6) + timedelta(hours=-8)
    clock_06_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=6) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=8) + timedelta(hours=-8)
    clock_08_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=8) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=10) + timedelta(hours=-8)
    clock_10_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=10) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=12) + timedelta(hours=-8)
    clock_12_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=12) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=14) + timedelta(hours=-8)
    clock_14_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=14) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=16) + timedelta(hours=-8)
    clock_16_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=16) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=18) + timedelta(hours=-8)
    clock_18_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=18) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=20) + timedelta(hours=-8)
    clock_20_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=20) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=22) + timedelta(hours=-8)
    clock_22_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    this_start_hour = this_startDt + timedelta(hours=22) + timedelta(hours=-8)
    this_end_hour = this_startDt + timedelta(hours=24) + timedelta(hours=-8)
    clock_24_count += db_marketanalysis.search_log2.count_documents({'class_code': {'$in': ['R', 'F']}, 'since':{'$gte': this_start_hour, '$lte': this_end_hour}})

    if (hour_month != this_startDt.month):
        yearMonth = str((this_startDt + relativedelta(months=-1)).year) + '-' + str((this_startDt + relativedelta(months=-1)).month);

        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '0-2時', 'year_month': yearMonth},
            {'$set' : {'period' : '0-2時', 'behavior_count': clock_18_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '2-4時', 'year_month': yearMonth},
            {'$set' : {'period' : '2-4時', 'behavior_count': clock_20_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '4-6時', 'year_month': yearMonth},
            {'$set' : {'period' : '4-6時', 'behavior_count': clock_22_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '6-8時', 'year_month': yearMonth},
            {'$set' : {'period' : '6-8時', 'behavior_count': clock_24_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '8-10時', 'year_month': yearMonth},
            {'$set' : {'period' : '8-10時', 'behavior_count': clock_02_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '10-12時', 'year_month': yearMonth},
            {'$set' : {'period' : '10-12時', 'behavior_count': clock_04_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '12-14時', 'year_month': yearMonth},
            {'$set' : {'period' : '12-14時', 'behavior_count': clock_06_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '14-16時', 'year_month': yearMonth},
            {'$set' : {'period' : '14-16時', 'behavior_count': clock_08_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '16-18時', 'year_month': yearMonth},
            {'$set' : {'period' : '16-18時', 'behavior_count': clock_10_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '18-20時', 'year_month': yearMonth},
            {'$set' : {'period' : '18-20時', 'behavior_count': clock_12_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '20-22時', 'year_month': yearMonth},
            {'$set' : {'period' : '20-22時', 'behavior_count': clock_14_count, 'year_month': yearMonth}},
            upsert=True
        )
        db_marketanalysis.learning_behavior_by_dayperiod.update_one(
            {'period' : '22-24時', 'year_month': yearMonth},
            {'$set' : {'period' : '22-24時', 'behavior_count': clock_16_count, 'year_month': yearMonth}},
            upsert=True
        )

        hour_month = this_startDt.month
        clock_02_count = 0
        clock_04_count = 0
        clock_06_count = 0
        clock_08_count = 0
        clock_10_count = 0
        clock_12_count = 0
        clock_14_count = 0
        clock_16_count = 0
        clock_18_count = 0
        clock_20_count = 0
        clock_22_count = 0
        clock_24_count = 0

    start_dt = this_endDt

db_marketanalysis.lastlog.update_one({'type': 'learning_behavior_analysis'}, {'$set': {'id': lastLog_id}}, upsert=True)

cursor.close()
client.close()