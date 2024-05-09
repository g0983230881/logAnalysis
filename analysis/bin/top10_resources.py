#!/usr/bin/python3.8
import sys, codecs
from collections import Counter
from pymongo import MongoClient
import datetime as dt
import pymysql
sys.path.append("..")
from settings import *


def tryInt(source):
	try:
		return int(source)
	except:
		return 0

# mysql connection
#conn = pymysql.connect(host='localhost', database='edumarket', user='', password='', charset='utf8')
conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cmd = conn.cursor(cursor=pymysql.cursors.DictCursor)

cmd.execute("select id from resources where isdelete='N' and workflow_id='WK2005'")
all_rid = [rec['id'] for rec in cmd.fetchall()]

# mongodb connection
#client = MongoClient()
#db = client[mongo_dbname_edumarket]
client = MongoClient(mongo_hostname, monggo_port)
db = client[mongo_dbname]
col_search_log = db['search_log2']

# parameters prepare
if len(sys.argv) < 3:
	print("usage: top10_resources.py begin-date end-date")
begin_date = dt.datetime.strptime(sys.argv[1], '%Y-%m-%d')
end_date = dt.datetime.strptime(sys.argv[2], '%Y-%m-%d') + dt.timedelta(days=1)

# statistic step
docs_rsc_usage = [doc for doc in col_search_log.find({
		"class_code": { "$in": ["R", "M_R"]},
		"since": { "$gt": begin_date, "$lte": end_date}
	}, {"main_col": 1})]
print('docs_rsc_usage find finished.')

rsc_ids = filter(lambda x: x!=0, [tryInt(doc['main_col']) for doc in docs_rsc_usage]) # filter error number (as 0)
rsc_usage_group = Counter(rsc_ids) # dictionary: [{ rid: usage_count }, ...]

zero_usage = len(set(all_rid) - set(rsc_usage_group.keys())) # all resources_id disjoin usage resources_id
once_usage = len({k:v for k,v in rsc_usage_group.items() if v == 1})

top10_rsc = sorted(rsc_usage_group.items(), key=lambda x: x[1], reverse=True)[:10]
top10_rsc_ids = [i[0] for i in top10_rsc]
print('top10_rsc sorted finished.')

# write to file
fw = codecs.open('../output/top_10_list.txt', 'w', 'utf8')
fw.write('rid\tusage\ttitlle\n')
for rsc_id in top10_rsc_ids:
	# print(rsc_id, rsc_usage_group[rsc_id])
	cmd.execute("select title from resources where id = %s", (rsc_id))
	rec = cmd.fetchone()
	fw.write(u"{}\t{}\t{}\n".format(rsc_id, rsc_usage_group[rsc_id], rec['title']))

fw.write("\n")
fw.write("Total Resources: {}\n".format(len(all_rid)))
fw.write("1 usage: {}\n".format(once_usage))
fw.write("0 usage: {}\n".format(zero_usage))
fw.close()

conn.close()
client.close()
print('top10_resources finished, output file directory at analysis/output/top_10_list.txt')