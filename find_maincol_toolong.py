#!/usr/bin/python3.8
# -*- coding: utf-8 -*-

from pymongo import MongoClient
import sys
sys.path.append("..")
from settings import *

client = MongoClient(mongo_hostname, monggo_port)
db = client[mongo_dbname]
col_search = db.search_log2

mainColCount = 0
datas = col_search.find()
for data in datas:
	main_col = data['main_col']
	if len(main_col) > 100:
		#col_search.update_one({'main_col': main_col}, {'$set': {'main_col': 'unknown'}})
		print(data['class_code'] + '  ' + main_col)
		mainColCount += 1
print('mainColCount=' + str(mainColCount))

classCodeCount = 0
datas = col_search.find()
for data in datas:
	class_code = data['class_code']
	if len(class_code) > 50:
		#col_search.update_one({'class_code': class_code}, {'$set': {'class_code': 'unknown'}})
		classCodeCount += 1
print('classCodeCount=' + str(classCodeCount))
