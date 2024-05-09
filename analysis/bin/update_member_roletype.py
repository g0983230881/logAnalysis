#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：
#功能說明：更新會員身份資料到mysql
#輸入參數：無
#資料來源：mongodb->educloud_loginhistory
#輸出結果：

from pymongo import MongoClient
from importlib import reload
import sys
import pymysql

sys.path.append("..")
from settings import *
reload(sys)

client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]

conn = pymysql.connect(host=mysql_hostname, database=mysql_dbname, user=mysql_user, passwd=mysql_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)


memberlogin_datas = db_edumarket.educloud_loginhistory.find({'titles':{'$exists':True}})
for memberlogin in memberlogin_datas:
    user_uuid = memberlogin['UserId']
    titles = memberlogin['titles']

    if titles.find(u'教師')!=-1 or titles.find(u'導師')!=-1 or titles.find('主任')!=-1 or titles.find(u'校長')!=-1 :
        SQL = "update member set roletype='00' where idtype='E' and uuid = '%s' "%(user_uuid)
        cursor.execute(SQL)
        conn.commit()
        print(user_uuid + '-------' + SQL + " -> " + str(cursor.rowcount))
    elif titles.find(u'學生')!=-1 :
        SQL = "update member set roletype='03' where idtype='E' and uuid = '%s' "%(user_uuid)
        cursor.execute(SQL)
        conn.commit()
        print(user_uuid + '-------' + SQL + " -> " + str(cursor.rowcount))
    else :
        print(user_uuid + '---------->' + titles)

cursor.close()
client.close()

