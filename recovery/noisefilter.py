#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：noisefilter.py
#功能說明：將線上的使用者行為記錄(db:edumarket / collection:searchlog)複製到專門的數據分析資料庫,
#          並進行雜訊的濾除,每次執行時都會從上次最後處理的log接續進行處理.
#輸入參數：無
#資料來源：mongoDB:edumarket -> searchlog
#輸出結果：mongoDB:marketanalysis -> search_log2
#開發人員：Chi-Wen Fann

from pymongo import MongoClient
from os.path import dirname
import sys
import netaddr
sys.path.append("..")
from settings import *


#取得上次處理log的最後ObjectId
def getLastlogId():
	lastLogId = ""
	lastlog_data = db_marketanalysis.lastlog.find_one({'type': 'searchlog_lastId'})
	try:
		lastLogId = lastlog_data['id']
		lastLogId2 = list(search_log.aggregate([{'$group':{'_id':'', 'last':{'$max':"$_id"}}}]))[0]['last']
		if (lastLogId != lastLogId2):
			lastLogId = lastLogId2
	except:
		lastLogId = ""
	return lastLogId

#
def readAllCrawlerIP():
	crawlerList = ['baidu.dat', 'bing.dat', 'facebook.dat', 'google.dat', 'sogou.dat']
	crawlerIP = []
	for crawler in crawlerList:
		with open(currentDirectory +  "/data/" + crawler) as input_data:	
			for ipset in input_data.readlines():
				ipset = ipset.strip()
				if (ipset.startswith('#') == True):
					continue
				crawlerIP.append(ipset)
	return crawlerIP


def readTestingUserIP():
	testingUserIP = []
	with open(currentDirectory +  "/data/" + 'owner.dat') as input_data:	
		for ipset in input_data.readlines():
			ipset = ipset.strip()
			if (ipset.startswith('#') == True):
				continue
			testingUserIP.append(ipset)
	return testingUserIP


def readAttackPattern():
	attackPattern = []
	with open(currentDirectory +  "/data/" + 'attack.dat') as input_data:	
		for attack in input_data.readlines():
			attack = attack.strip()
			if (attack.startswith('#') == True):
				continue
			attackPattern.append(attack)
	return attackPattern


#def crawlerfilter():
def filter_testingUser(edumarket_testingUser, edumarket_testingUSerIP, searchlog):
	isTestingUser = False
	if (searchlog['user_id'] in edumarket_testingUser) :
		isTestingUser = True
	if isTestingUser == True :
		for  ip in netaddr.IPSet(edumarket_testingUSerIP) :			
			if (searchlog['from_ip'] == str(ip)) :
				#print(str(ip) + '----------' + searchlog['from_ip'])
				isTestingUser = True
				break
	return isTestingUser

#def errorFilter():
#def intrusionFilter():



if __name__ == "__main__" :
	client = MongoClient(mongo_hostname, monggo_port)
	db_from = client[mongo_dbname_edumarket]
	db_marketanalysis = client[mongo_dbname_marketanalysis]
	searchlog = db_from.searchlog
	search_log = db_marketanalysis.search_log2
	identify_member = db_marketanalysis.identify_member
	
	currentDirectory = dirname(__file__)
	if len(currentDirectory) == 0 :
		currentDirectory = "."
	edumarket_testingUser = [13834,44607,45223,64322,64323,67059,73772,73926,73966,81530,82064,82153,91435,80315,80421,81870,81943,93446,94282]
	edumarket_testingUSerIP = readTestingUserIP()
	crawler_IP = readAllCrawlerIP()
	AttackPattern = readAttackPattern()

	#從searchlog前次處理後的位置接續進行處理
	lastLogId = getLastlogId()
	#count = 0
	filter = {}
	if lastLogId != "":
		filter = {'_id': {'$gt': lastLogId}}
		searchlog_data = searchlog.find(filter, no_cursor_timeout=True)
		for _searchlog in searchlog_data:
			#寫入使用者追踪及識別資料
			if len(_searchlog['session_id']) > 0:
				#相同的使用者session_id,cookie_iden,user_id,只寫入一筆記錄
				identify_member.update_one(
					{'user_id': _searchlog['user_id'], 'session_id': _searchlog['session_id'], 'cookie_iden': _searchlog['cookie_iden']},{"$set":
					{'user_id': _searchlog['user_id'], 'session_id': _searchlog['session_id'], 'cookie_iden': _searchlog['cookie_iden']}},
					upsert=True)

			#進行各項的filter處理,以排除不需要分析的log
			#濾除測試資料
			isTestingUser = filter_testingUser(edumarket_testingUser, edumarket_testingUSerIP, _searchlog)
			if (isTestingUser == True):
				#print(_searchlog['_id'])
				continue

			#濾除爬蟲程式產生的資料
			if (_searchlog['from_ip'] in crawler_IP) :
				#print('IP--->' + str(_searchlog['_id']) + '   ' + str(_searchlog['from_ip']))
				continue

			#濾除入侵行為產生的資料
			if (_searchlog['sec_col'] in AttackPattern) :
				continue

			#將沒有被過濾的日誌記錄儲存
			search_log.insert(_searchlog)
			db_marketanalysis.lastlog.update_one({'type':'searchlog_lastId'},{"$set":{'id':_searchlog['_id'],'type':'searchlog_lastId'}},upsert=True)

			'''
			#寫入使用者追踪及識別資料
			if len(_searchlog['session_id']) > 0:
						#相同的使用者session_id,cookie_iden,user_id,只寫入一筆記錄
						identify_member.update_one(
							{'user_id': _searchlog['user_id'], 'session_id': _searchlog['session_id'], 'cookie_iden': _searchlog['cookie_iden']}, {"$set":
							{'user_id': _searchlog['user_id'], 'session_id': _searchlog['session_id'], 'cookie_iden': _searchlog['cookie_iden']}},
							upsert=True)
			'''

			"""count = count + 1
			if count > 10 :
				break"""
		

		#建立search_log的index
		if 'user_id' not in search_log.index_information() :
			search_log.create_index('user_id')
		if 'session_id' not in search_log.index_information() :
			search_log.create_index('session_id')
		if 'class_code' not in search_log.index_information() :
			search_log.create_index('class_code')
		if 'cookie_iden' not in search_log.index_information() :
			search_log.create_index('cookie_iden')
		if 'since' not in search_log.index_information() :
			search_log.create_index('since')

		#建立identify_member的index
		if 'user_id' not in identify_member.index_information() :
			identify_member.create_index('user_id')
		if 'session_id' not in identify_member.index_information() :
			identify_member.create_index('session_id')
		if 'cookie_iden' not in identify_member.index_information() :
			identify_member.create_index('cookie_iden')

		#利用已登入的使用者session_id及cookie_iden,找出未登入時的使用者記錄
		identify_member_datas = identify_member.find({'user_id' : {'$gt' : 0}})
		for identify_member_data in identify_member_datas:
			user_id = identify_member_data['user_id']
			session_id = identify_member_data['session_id']
			cookie_iden = identify_member_data['cookie_iden']

			identify_member.update_one(
				{'session_id' : session_id, 'user_id' : {'$lte' : 0}},
				{'$set' : {'user_id' : user_id, 'identify' : True}}, 
				upsert=False)

			identify_member.update_one(
				{'cookie_iden' : cookie_iden, 'user_id' : {'$lte' : 0}},
				{'$set' : {'user_id' : user_id, 'identify' : True}}, 
				upsert=False)

		client.close()
