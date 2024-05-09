#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：keyword_analysis.py
#功能說明：查詢關鍵字分析, 計算不同學制與資源類型頻率最高的關鍵字, 作為該類推薦
#輸入參數：
#資料來源：searchlog
#輸出結果：marketanalysis -> hot_keyword_by_type
#開發人員：Derek

from pymongo import MongoClient
from collections import Counter
from urllib import *
import datetime, urllib, pymysql
import sys, operator
import string
from urllib.parse import quote
sys.path.append("..")
from settings import *

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
client = MongoClient(mongo_hostname, monggo_port)
db_marketanalysis = client[mongo_dbname_marketanalysis]
db_edumarket = client[mongo_dbname_edumarket]

keywordFreqList = db_edumarket.searchlog.aggregate(
   [
	  {
		'$match' : {
		   'class_code': {'$in': ['Q','QSK']}
		},
	  },
	  {
		'$group' : {
		   '_id' : { 'keyword' : '$sec_col' },
		   'count': { '$sum': 1 }
		}
	  },
	  { '$sort' : { 'count' : -1, 'since' : -1 } }
   ]
)
print('aggregate finished.')

topKeyword = {}
topKeywordSet = {}
i=1
totalCount = 0
for doc in list(keywordFreqList) :
	totalCount= totalCount + doc['count']  
	if i <= 15000 : 
		key = doc['_id']['keyword']
		topKeyword[key] = doc['count']
	i = i + 1

lowerset = set(k.lower() for k in topKeyword)
for kl in lowerset:
	for k, v in topKeyword.items():
		if kl == k.lower():
			topKeywordSet[kl] = v 
		elif kl == k.lower() and kl in topKeywordSet.keys():
			topKeywordSet[kl] = topKeywordSet[kl] + v
# print(json.dumps(topKeywordSet, ensure_ascii=False, encoding='UTF-8'))

bmtype_dict = {}
edugrade_dict = {}
keywordType_dict = {}  ## 關鍵字屬性dict, 屬於何種學制or資源類型
count_break = 0
for keyword in topKeywordSet:
	try:
		bmtype_list = []
		edugrade_list = []
		type_list = []

		## 全文檢索中文字串, 需轉換成網頁可讀模式 urllib.quote()
		url = solrServer+'select?q=' + urllib.quote(keyword.encode('unicode_escape')) + '&wt=python&rows=20'
		connection = urllib.request.urlopen(quote(url, safe=string.printable))
		response = eval(connection.read())
		# print(response['response']['numFound'])
		for document in response['response']['docs']:
			_id = document['id']
			if len(_id) > 7:
				indexSlash = _id.rindex('/')
				_id = _id[indexSlash+1:]

			SQL1 = 'select bmtype, edugrade from resources where id=' + str(_id) + ' and isdelete="N"'
			try: 
				cursor.execute(SQL1)
				resources_datas = cursor.fetchall()
				for resources_data in resources_datas:
					bmtype_list.append(resources_data.get('bmtype'))
					edugrade_list.append(resources_data.get('edugrade')) 
			except:
				continue
 
		## 計算 list 內 duplicate value 有幾次
		bmtype_count_dict = dict(Counter(bmtype_list))
		edugrade_count_dict = dict(Counter(edugrade_list))

		## 取得 dict 中最大 integer value 的 key
		bmtype = max(bmtype_count_dict.items(), key=operator.itemgetter(1))[0]
		edugrade = max(edugrade_count_dict.items(), key=operator.itemgetter(1))[0]

		type_list.append(bmtype)
		type_list.append(edugrade)
		keywordType_dict[keyword] = type_list

	except:
		continue
# print(json.dumps(keywordType_dict, ensure_ascii=False, encoding='UTF-8'))
print('topKeywordSet loop finished.')

elem_count = 0
junior_count = 0
senior_count = 0
tech_count = 0
web_count = 0
ebook_count = 0
app_count = 0
elem_dict = {}
junior_dict = {}
senior_dict = {}
tech_dict = {}
web_dict = {}
ebook_dict = {}
app_dict = {}

sorted_topKeyword = sorted(topKeyword.items(), key=operator.itemgetter(1), reverse = True)
for keyword in sorted_topKeyword:
	for key, value in keywordType_dict.items():
		if keyword[0] == key:
			# print(str(keyword[0]) + '---' + str(keyword[1]))
			# print(value[0] + '-----' + value[1])
			if value[0] == 'web' and web_count < 30:
				web_dict[key] = keyword[1]
				web_count +=1
			if value[0] == 'ebook' and ebook_count < 30:
				ebook_dict[key] = keyword[1]
				ebook_count +=1
			if value[0] == 'app' and app_count < 30:
				app_dict[key] = keyword[1]
				app_count +=1
			if value[1] == 'B' and elem_count < 30 :
				elem_dict[key] = keyword[1]
				elem_count +=1
			if value[1] == 'C' and junior_count < 30 :
				junior_dict[key] = keyword[1]
				junior_count +=1
			if value[1] == 'D' and senior_count < 30 :
				senior_dict[key] = keyword[1]
				senior_count +=1
			if value[1] == 'E' and tech_count < 30 :
				tech_dict[key] = keyword[1]
				tech_count +=1
now = datetime.datetime.now()
dt_str = now.strftime("%Y-%m-%d")
print('sorted_topKeyword loop finished.')

db_marketanalysis.hot_keyword_by_type.update_one(
		{'bmtype' : 'web', 'year_month_day': dt_str},
		{'$set' : {'bmtype' : 'web', 'year_month_day': dt_str, 'top30keyword': web_dict}},
		upsert=True
	)

db_marketanalysis.hot_keyword_by_type.update_one(
		{'bmtype' : 'ebook', 'year_month_day': dt_str},
		{'$set' : {'bmtype' : 'ebook', 'year_month_day': dt_str, 'top30keyword': ebook_dict}},
		upsert=True
	)

db_marketanalysis.hot_keyword_by_type.update_one(
		{'bmtype' : 'app', 'year_month_day': dt_str},
		{'$set' : {'bmtype' : 'app', 'year_month_day': dt_str, 'top30keyword': app_dict}},
		upsert=True
	)

db_marketanalysis.hot_keyword_by_type.update_one(
		{'edugrade' : 'B', 'year_month_day': dt_str},
		{'$set' : {'edugrade' : 'B', 'year_month_day': dt_str, 'top30keyword': elem_dict}},
		upsert=True
	)

db_marketanalysis.hot_keyword_by_type.update_one(
		{'edugrade' : 'C', 'year_month_day': dt_str},
		{'$set' : {'edugrade' : 'C', 'year_month_day': dt_str, 'top30keyword': junior_dict}},
		upsert=True
	)

db_marketanalysis.hot_keyword_by_type.update_one(
		{'edugrade' : 'D', 'year_month_day': dt_str},
		{'$set' : {'edugrade' : 'D', 'year_month_day': dt_str, 'top30keyword': senior_dict}},
		upsert=True
	)

db_marketanalysis.hot_keyword_by_type.update_one(
		{'edugrade' : 'E', 'year_month_day': dt_str},
		{'$set' : {'edugrade' : 'E', 'year_month_day': dt_str, 'top30keyword': tech_dict}},
		upsert=True
	)

print('keyword_analysis finished.')
client.close()
conn.close()