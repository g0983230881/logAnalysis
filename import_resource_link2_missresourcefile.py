#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：import_resource_link2_missresourcefile.py
#功能說明：補匯入教學資源只有縮圖沒有實際附件的資源檔案(資源編號小於等於1696009)
#輸入參數：無
#資料來源：sync_resource_link2_missresourcefile2.txt
#輸出結果：無
#開發人員：Chi-Wen Fann

from importlib import reload
import os,sys
import pymysql
import os.path
import string,random
from shutil import copyfile

sys.path.append("..")
from settings import *


reload(sys)

#將資源編號轉成16進制的路徑
def resourcesIdToPath(resources_id):
	resourcesId = str(hex(resources_id))[2:]
	resourcesIdhex = resourcesId.rjust(8, '0')
	path = ''
	for i in range(0, len(resourcesIdhex), 2) :
		path += '/' + resourcesIdhex[i:i+2]
	return path

#若file_path的目錄不存在,則建立目錄
def ensure_dir(file_path):
	directory = os.path.dirname(file_path)
	if not os.path.exists(directory):
		os.makedirs(directory)


def secret_generator(size=8, chars=string.digits):
	return ''.join(random.choice(chars) for _ in range(size))


conn = pymysql.connect(host=mysql_hostname, database=mysql_dbname, user=mysql_user, passwd=mysql_password, charset='utf8')
#conn2 = pymysql.connect(host=mysql_hostname, database='portal', user=mysql_user, passwd=mysql_password, charset='utf8')
#ispcursor1 = conn2.cursor(cursor=pymysql.cursors.DictCursor)

ResourceSQL = "select id,title,`desc`,bmtype,workflow_id,member_id from resources where isdelete='N' and workflow_id in ('WK2005','WK2008') and id=%s"
ResourceObjSQL = "select * from resources_objfile where isdelete='N' and resources_id={0}"
ObjectFileSQL  = "select * from objectfile where isdelete='N' and id={0}"
#ISPResourceSQL = "select * from resources where rno=%s"
insertObjectFileSQL = "insert into objectfile(member_id,type,title,filename,secret,filesize,created_at,updated_at) values(%s,'R','',%s,%s,%s,now(),now())"
insertResources_objfileSQL = "insert into resources_objfile(resources_id,objectfile_id,member_id,created_at,updated_at) values(%s,%s,%s,now(),now())"
insertFolder_objfileSQL = "insert into folder_objfile(folder_id,objectfile_id,member_id,created_at,updated_at) values(%s,%s,%s,now(),now())"
selectFolderSQL = "select folder_id from folder_resource where isdelete='N' and resources_id=%s"
#檢查資源附件是否已存在的語法
SQL = "select * from resources_objfile where resources_id=%s and isdelete='N' and objectfile_id in (select id from objectfile where filename=%s and member_id=%s and isdelete='N')"
#資源下架語法
unPublishSQL = "update resources set workflow_id='WK2007' where id=%s and isdelete='N' and workflow_id in ('WK2005','WK2008') "
unPublishReasonSQL = "insert into unpublish(resources_id,users_id,reason,created_at) values(%s,%s,%s,now())"

file = open("../conf/sync_resource_link2_missresourcefile.txt","r")
for line in file.readlines():
	line = line.strip()
	if not len(line) or line.startswith('#') :
		continue
	data = line.split(' ')
	resources_id = data[0]
	filename = data[1]
	if (filename.rfind('/') > 0) :
		filename = filename[filename.rfind('/')+1:]
	cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
	cursor1.execute(ResourceSQL, (int(resources_id)))
	resourcesRow = cursor1.fetchone()
	if resourcesRow is not None :
		member_id = resourcesRow.get('member_id')
		cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
		cursor2.execute(SQL, (resources_id, filename, member_id))
		objfileExists = cursor2.fetchall()
		if len(objfileExists) > 0 :
			print(resources_id + ' has ' + str(len(objfileExists)) + ' relation item ' + filename + ' ' + str(member_id))
			'''
			if objfileExists.get('resources_id') == int(resources_id) :
				#要匯入的那一筆資源附件已存在,不需處理
				print(resources_id + ' attachment exist')
				print(resources_id + ' <-> ' + str(objfileExists[0].get('resources_id')) + ' diff ' + filename)
			else :
				#檢查到資源附件已存在,但屬於另外一筆資源,但該筆資源品質較差予以下架,保留沒有資源附件(補上附件)的這筆
				cursor3.execute(unPublishSQL, (resources_id))
				
				cursor3.execute(unPublishReasonSQL, (resources_id, 1, '與 '+ str(objfileExists.get('resources_id')) + ' 資源重複'))
				
				thumbSrc = '/www/edudata/thumbnail' + resourcesIdToPath(int(resources_id)) + '_thumb.png'
				thumbDest = '/www/edudata/thumbnail' + resourcesIdToPath(objfileExists.get('resources_id')) + '_thumb.png'
				if os.path.exists(thumbSrc) == True and os.path.exists(thumbDest) == False:
					ensure_dir(thumbDest)
					copyfile(thumbSrc, thumbDest)
					print('Copy ' + thumbSrc + ' to ' + thumbDest + ' success')
				elif os.path.exists(thumbSrc) == False and os.path.exists(thumbDest) == False :
					print(thumbSrc + ' File not found')
				conn.commit()
				cursor3.close()
			'''
		else :
			#需匯入資源附件的處理			
			attachSrc = '/mnt/smb1/isp/docs/edshare/uploads/'
			if os.path.exists(attachSrc + data[1]) == True :
				#建立objectfile主記錄
				filesize = os.path.getsize(attachSrc + data[1])
				secret = secret_generator()
				cursor3 = conn.cursor(cursor=pymysql.cursors.DictCursor)
				cursor3.execute(insertObjectFileSQL, (member_id, filename, secret, filesize))
				if cursor3.lastrowid :
					#成功取得建立的記錄編號才進行檔案複製及建立關連記錄
					objectfile_id = cursor3.lastrowid
					#複製實體檔案
					filetype = filename[filename.rfind('.'):]
					dest = '/www/edudata/resource/' + resourcesIdToPath(objectfile_id) + '_' + secret + filetype
					ensure_dir(dest)
					copyfile(attachSrc + data[1], dest)
						
					#建立關連資料
					cursor3.execute(insertResources_objfileSQL, (resources_id, objectfile_id, member_id))
					cursor4 = conn.cursor(cursor=pymysql.cursors.DictCursor)
					cursor4.execute(selectFolderSQL, (resources_id))
					folderRow = cursor4.fetchone()
					if folderRow is not None :
						folder_id = folderRow.get('folder_id')
						cursor3.execute(insertFolder_objfileSQL, (folder_id, objectfile_id, member_id))
					else :
						print(resources_id + ' not folder id')
					cursor4.close()
				else :
					print(resources_id + ' insert objectfile fail')
				conn.commit()
				cursor3.close()
				print(resources_id + ' ' + str(filesize) + ' ' + secret)		
			else :
				print(resources_id + ' not found')


		cursor2.close()
	cursor1.close()
file.close()
conn.close()
