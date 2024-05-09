#!/usr/bin/python3.8
import sys
import datetime as dt
from pymongo import MongoClient
import pymysql

sys.path.append("..")
from settings import *

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)

settings = eval(open('../settings.json').read())
client = MongoClient(settings['mghost'], settings['mgport'])
db = client[ settings['mg_db_name'] ]
col_interc_session = db['interc_session']
col_member = db['member']


# check a session's logined type
# input:  aggregated session log
# output: 
def checkSessionLoginedInfo(log):
	user_list = log['logs']['user_list']
	if -1 in user_list:
		user_list.remove(-1)
	if user_list == []:
		return 'guest', 0
	# elif len(user_list) > 1:
	# 	return -1 # multi-user execption
	else:
		try:
			#member_info = [i for i in col_member.find({'id':user_list[0]})][0]
			member_info = getmemberInfo(user_list[0])
			idtype, city_id = member_info['idtype'], member_info['city_id']
			return idtype, city_id
		except:
			return 'err', -1

def getmemberInfo(member_id):
	SQL = "select id,idtype,city_id from member where id={0}"
	cursor1.execute(SQL.format(member_id))
	memberinfo = cursor1.fetchone()
	return memberinfo

### parameters preprocessing 
# [date] yyyy-mm-dd daily-record for process
arg_begin_dt = sys.argv[1]
begin_date = dt.datetime.strptime(arg_begin_dt, "%Y-%m-%d")
end_date = begin_date + dt.timedelta(days=1)

logs = [i for i in col_interc_session.aggregate([
		{ "$match": { "date": begin_date } },
		{ "$unwind": "$logs" }
	])]

period_m = begin_date.strftime('%Y-%m')
print('session: [date: {0}], logs: {1}'.format(begin_date.strftime("%Y-%m-%d"), len(logs)))
for log in logs:
	idtype, city_id = checkSessionLoginedInfo(log)
	# print(idtype, city_id)
	period_d = begin_date.strftime('%Y-%m-%d')
	# print(period_d, period_m)
	if idtype == 'I':
		db['city_traffic_day'].update_one({'period':period_d, 'city_id': city_id }, { '$inc': { 't_isp_session': 1 }}, upsert=True)
		db['city_traffic_month'].update_one({'period':period_m, 'city_id': city_id }, { '$inc': { 't_isp_session': 1 }}, upsert=True)
		db['traffic_day'].update_one({'period':period_d }, { '$inc': { 't_isp_session': 1 }}, upsert=True)
		db['traffic_month'].update_one({'period':period_m }, { '$inc': { 't_isp_session': 1 }}, upsert=True)
	elif idtype == 'E':
		db['city_traffic_day'].update_one({'period':period_d, 'city_id': city_id }, { '$inc': { 't_openid_session': 1 }}, upsert=True)
		db['city_traffic_month'].update_one({'period':period_m, 'city_id': city_id }, { '$inc': { 't_openid_session': 1 }}, upsert=True)
		db['traffic_day'].update_one({'period':period_d }, { '$inc': { 't_openid_session': 1 }}, upsert=True)
		db['traffic_month'].update_one({'period':period_m }, { '$inc': { 't_openid_session': 1 }}, upsert=True)
	elif idtype in ['F','G','Y','O']:
		db['city_traffic_day'].update_one({'period':period_d, 'city_id': city_id }, { '$inc': { 't_social_session': 1 }}, upsert=True)
		db['city_traffic_month'].update_one({'period':period_m, 'city_id': city_id }, { '$inc': { 't_social_session': 1 }}, upsert=True)
		db['traffic_day'].update_one({'period':period_d }, { '$inc': { 't_social_session': 1 }}, upsert=True)
		db['traffic_month'].update_one({'period':period_m }, { '$inc': { 't_social_session': 1 }}, upsert=True)
	elif idtype == 'guest':
		db['traffic_day'].update_one({'period':period_d }, { '$inc': { 't_guest_session': 1 }}, upsert=True)
		db['traffic_month'].update_one({'period':period_m }, { '$inc': { 't_guest_session': 1 }}, upsert=True)
	else:
		pass
