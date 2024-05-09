#!/usr/bin/python3.8
"""
usage: construct_intermedia_collection.py (begin_date) [end_date]"
	begin_date (date)
	end_date (date, option): if no end_date, the default is begin_date + 1 day

* [date] format yyyy-mm-dd
* [mode] (optional), a: all, c: cookie, s: session, r: resources
"""

from pymongo import MongoClient
import sys
import datetime as dt

if len(sys.argv) == 1:
	print("""
	usage:
		python construct_intermedia_collection.py [date] [mode]
		- date: format yyyy-mm-dd
		- mode (optional): a, c, s, r [all, cookie, session, resources]
	""")

### parameters preprocessing 
# [date] yyyy-mm-dd daily-record for process
arg_begin_dt = sys.argv[1]
begin_date = dt.datetime.strptime(arg_begin_dt, "%Y-%m-%d")
end_date = begin_date + dt.timedelta(days=1)

# [mode] specify a mode parameter
if len(sys.argv) > 2:
	arg_mode = sys.argv[2]
else:
	arg_mode = 'a'

# define necessary collections
settings = eval(open('../settings.json').read())
client = MongoClient(settings['mghost'], settings['mgport'])
db = client[ settings['mg_db_name'] ]
col_search_log = db['search_log2']

col_cookie_statist = db['interc_cookie']
col_session_statist = db['interc_session']
col_resources_statist = db['interc_resource']

### aggregate by cookie
if arg_mode in ['c', 'a']:
	docs = [ doc for doc in 
		col_search_log.aggregate([
			{
				"$match": { "since": { "$gte": begin_date, "$lt": end_date } }
			},
			{
				"$group": {
					"_id": "$cookie_iden",
					"first_since": { "$min": "$since" }, 
					"last_since": { "$max": "$since" },
					"log_count": { "$sum": 1 },
					"behavior_list": { "$push": "$class_code" },
					"user_list": { "$addToSet": "$user_id" },
					"session_list": { "$addToSet": "$session_list" }
				}
			},
			{ "$sort": { "since": 1 } }
			# { "$out": "interc_cookie" }
		]).batch_size(100) 
	]
	print(begin_date.strftime("%Y-%m-%d"), 'cookie', len(docs))
	#col_cookie_statist.insert(dict(date=begin_date, logs=docs))
	col_cookie_statist.update_one({"date":begin_date},{"$set":{"logs":docs}},upsert=True)
	status = db.command('getlasterror')['updatedExisting']
	print(status)

### aggregate by session
if arg_mode in ['s', 'a']:
	docs = [ doc for doc in 
		col_search_log.aggregate([
			{
				"$match": { "since": { "$gte": begin_date, "$lt": end_date } }
			},
			{
				"$group": {
					"_id": "$session_id",
					"first_since": { "$min": "$since" }, 
					"last_since": { "$max": "$since" },
					"log_count": { "$sum": 1 },
					"behavior_list": { "$push": "$class_code" },
					"user_list": { "$addToSet": "$user_id" },
					"cookie_list": { "$addToSet": "$cookie_list" }
				}
			},
			{ "$sort": { "since": 1 } }
			# { "$out": "interc_session" }
		]).batch_size(100) 
	]
	print(begin_date.strftime("%Y-%m-%d"), 'session', len(docs))
	#col_session_statist.insert(dict(date=begin_date, logs=docs))
	col_session_statist.update_one({"date":begin_date},{"$set":{"logs":docs}},upsert=True)
	status = db.command('getlasterror')['updatedExisting']
	print(status)

### aggregate by resources
if arg_mode in ['r', 'a']:
	docs = [ doc for doc in 
		col_search_log.aggregate([
			{
				"$match": { 
					"since": { "$gte": begin_date, "$lt": end_date },
					"class_code": { "$in": ["R", "M_R"]}
				}
			},
			{
				"$group": {
					"_id": "$main_col",
					"first_since": { "$min": "$since" }, 
					"last_since": { "$max": "$since" },
					"log_count": { "$sum": 1 },
					"behavior_list": { "$push": "$class_code" },
					"user_list": { "$addToSet": "$user_id" },
					"session_list": { "$addToSet": "$session_id" }
				}
			},
			{ "$sort": { "since": 1 } }
			# { "$out": "interc_resource" }
		])
	]
	print(begin_date.strftime("%Y-%m-%d"), 'resources', len(docs))
	#col_resources_statist.insert(dict(date=begin_date, logs=docs))
	col_resources_statist.update_one({"date":begin_date},{"$set":{"logs":docs}},upsert=True)
	status = db.command('getlasterror')['updatedExisting']
	print(status)
