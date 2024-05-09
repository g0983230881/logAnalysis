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
col_search_log = db['search_log2']
col_member = db['member']


### parameters preprocessing 
# [date] yyyy-mm-dd daily-record for process
arg_begin_dt = sys.argv[1]
begin_date = dt.datetime.strptime(arg_begin_dt, "%Y-%m-%d")
end_date = begin_date + dt.timedelta(days=1)

behavior_type = eval(open('../conf/behavior_type').read())
idtype_map = {
	'I': 'isp',
	'E': 'openid',
	'F': 'social',
	'G': 'social',
	'Y': 'social',
	'O': 'social',
	'guest': 'guest',
	'err': 'err'
}

# check a session's logined type
# input:  aggregated session log
# output: 
def checkSessionLoginedInfo(log):
	user_id = log['user_id']
	if user_id == -1:
		return 'guest', 0
	else:
		try:
			#member_info = [i for i in col_member.find({'id':user_id})][0]
			member_info = getmemberInfo(user_id)
			idtype, city_id = member_info['idtype'], member_info['city_id']
			return idtype, city_id
		except:
			return 'err', -1

def getmemberInfo(member_id):
        SQL = "select id,idtype,city_id from member where id={0}"
        cursor1.execute(SQL.format(member_id))
        memberinfo = cursor1.fetchone()
        return memberinfo

def CheckBType(bcode):
	for bt in behavior_type:
		if bcode in bt:
			return behavior_type.index(bt) + 1

logs = [i for i in col_search_log.find({"since": { "$gte": begin_date, "$lt": end_date}}).batch_size(100)]
period_m = begin_date.strftime('%Y-%m')
print('  usage: [date: {0}], logs: {1}'.format(begin_date.strftime("%Y-%m-%d"), len(logs)))
for log in logs:
	idtype, city_id = checkSessionLoginedInfo(log)
	bcode = log['class_code']
	period_d = begin_date.strftime('%Y-%m-%d')
	if idtype == 'err':
		continue
	if idtype != 'guest':
		db['city_traffic_day'].update_one({'period':period_d,   'city_id': city_id }, { '$inc': { 't_{0}_c{1}'.format(idtype_map[idtype], CheckBType(bcode)): 1, 't_{0}_all'.format(idtype_map[idtype]): 1 }}, upsert=True)
		db['city_traffic_month'].update_one({'period':period_m, 'city_id': city_id }, { '$inc': { 't_{0}_c{1}'.format(idtype_map[idtype], CheckBType(bcode)): 1, 't_{0}_all'.format(idtype_map[idtype]): 1 }}, upsert=True)
	db['traffic_day'].update_one({  'period':period_d }, { '$inc': { 't_{0}_c{1}'.format(idtype_map[idtype], CheckBType(bcode)): 1, 't_{0}_all'.format(idtype_map[idtype]): 1 }}, upsert=True)
	db['traffic_month'].update_one({'period':period_m }, { '$inc': { 't_{0}_c{1}'.format(idtype_map[idtype], CheckBType(bcode)): 1, 't_{0}_all'.format(idtype_map[idtype]): 1 }}, upsert=True)
	
conn.close()