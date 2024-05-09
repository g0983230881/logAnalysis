#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：sync_resource_link.py
#功能說明：檢查教學資源(資源編號小於等於1696009)的連結或附件是否相同
#輸入參數：無
#資料來源：edumarket->resources , portal->resources
#輸出結果：sync_resource_link.txt
#開發人員：Chi-Wen Fann

from importlib import reload
import sys
import pymysql

sys.path.append("..")
from settings import *
reload(sys)

conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor2 = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor3 = conn.cursor(cursor=pymysql.cursors.DictCursor)
conn2 = pymysql.connect(host=mysqlRead_hostname, database='portal', user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
ispcursor1 = conn2.cursor(cursor=pymysql.cursors.DictCursor)

ResourceSQL = "select id,title,`desc`,bmtype,workflow_id,member_id from resources where isdelete='N' and workflow_id in ('WK2005','WK2008') and id<=1696009"
ResourceObjSQL = "select * from resources_objfile where isdelete='N' and resources_id={0}"
ObjectFileSQL  = "select * from objectfile where isdelete='N' and id={0}"
ISPResourceSQL = "select * from resources where rno=%s"

renameAttach = open("../output/sync_resource_attach_rename.txt","w+")
resfile1 = open("../output/sync_resource_link.txt","w+")
cursor1.execute(ResourceSQL)
resourcesRows = cursor1.fetchall()
for resources in resourcesRows :
	resources_id = resources.get('id')

	ispcursor1.execute(ISPResourceSQL, (resources_id))
	ispResourceRow = ispcursor1.fetchone()
	if ispResourceRow is not None:
		cursor2.execute(ResourceObjSQL.format(resources_id))
		resourcesObjRows = cursor2.fetchall()
		for resourcesobj in resourcesObjRows :
			objectfile_id = resourcesobj.get('objectfile_id')
	
			cursor3.execute(ObjectFileSQL.format(objectfile_id))
			objectfileRow = cursor3.fetchone()
			if objectfileRow is not None:			
				objectType = objectfileRow.get('type')
				objectName = objectfileRow.get('filename')

				#比對兩邊資源連結及附件的數目是否相等
				file_type = ispResourceRow.get('file_type').lower()
				edumarketCount = len(resourcesObjRows);
				ispCount = 0
				file_name = ispResourceRow.get('file_name')
				file_name_inside = ispResourceRow.get('file_name_inside')
				resources_uri2 = ispResourceRow.get('resources_uri2')
				if (file_name is not None and file_name!='') :
					ispCount += 1
				if (file_name_inside is not None and file_name_inside!='' and file_name_inside.rfind('.')>0) :
					ispCount += 1
				if (resources_uri2 is not None and resources_uri2!='') :
					ispCount += 1
				if (edumarketCount != ispCount) :
					if (edumarketCount == 1 and (objectName.lower().rfind('.jpg')>0 or objectName.lower().rfind('png')>0) and file_type.find('png')<0 and file_type.find('jpg')<0)  :
						resfile1.write(str(resources_id) + ' '  + ispResourceRow.get('resources_uri') + file_name + file_type + ' ' + str(edumarketCount) + ' ' + str(ispCount) + ' only image\n')
					else :
						resfile1.write('resources ' + str(resources_id) + ' diff ' + objectType + ' ' + str(edumarketCount) + ' ' + str(ispCount) + '\n')

				if (objectType == 'U') :
					#資源連結的檢查比對
					if (objectName == ispResourceRow.get('resources_uri2')) :	
						continue
					else :
						if (objectName.find('/hsmaterial/') > 0 and ispResourceRow.get('resources_uri2').find('/hsmaterial.') > 0) :
							continue
						else :
							resfile1.write('resources 1 ' + str(resources_id) + ' ' +  objectName + ' ' +  ispResourceRow.get('resources_uri2') + '\n')
				elif (objectType == 'R'):
					#實體資源檔的檢查比對
					file_name = ispResourceRow.get('file_name')
					file_type = ispResourceRow.get('file_type')
					resources_uri = ispResourceRow.get('resources_uri')
					resources_uri2 = ispResourceRow.get('resources_uri2')
					file_name_old = ispResourceRow.get('file_name_old')
					file_name_inside = ispResourceRow.get('file_name_inside')

					if (file_name_old is None) :
						file_name_old = ''

					if (resources_uri2 is not None and resources_uri2.rfind('/') > 0) :
						resources_uri2 = resources_uri2[resources_uri2.rfind('/')+1:]
						resources_uri2_prefix = resources_uri2[0:resources_uri2.rfind('.')]
						resources_uri2_extend = resources_uri2[resources_uri2.rfind('.'):]
						resources_uri2 = resources_uri2_prefix + resources_uri2_extend.lower()

					if (file_name_inside is not None and file_name_inside.rfind('/') > 0) :
						file_name_inside = file_name_inside[file_name_inside.rfind('/')+1:]

					if file_type.startswith('.')==False :
						file_type = '.' + file_type
					file_type = file_type.lower()

					if (objectName.rfind('.') > 0) :
						objectName_prefix = objectName[0:objectName.rfind('.')]
						objectName_extend = objectName[objectName.rfind('.'):]
						objectName = objectName_prefix + objectName_extend.lower()

					if (objectName == (file_name + file_type)) :
						#檔案名稱與isp中的檔案名稱相同(file_name與file_type組合)
						if objectName.find('rename')>=0 and len(ispResourceRow.get('file_name_old'))>1:
							renameAttach.write('# resources_id=' + str(resources_id) + '\n')
							renameAttach.write('update objectfile set filename=\'' + ispResourceRow.get('file_name_old')+file_type + '\' where id=' + str(objectfileRow.get('id')) + ';\n')
						continue
					elif (objectName == (file_name_old + file_type)) :
						#檔案名稱與isp中的原始檔案名稱相同(file_name_old與file_type組合)
						continue
					elif (objectName == file_name_inside) :
						#檔案名稱與isp中的縮圖檔案名稱相同
						continue
					elif (objectName == resources_uri2) :
						#檔案名稱與isp中的連結網址檔名相同
						continue
					elif (file_name_inside is not None) :
						resfile1.write('resources 4 ' + str(resources_id) + ' ' +  objectName + ' ' +  file_name_inside + '\n')
					else :
						resfile1.write('resources 8 ' + str(resources_id) + ' ' +  objectName + ' - \n')
		
			else :
				resfile1.write(str(resources_id) + ' - ' + str(objectfile_id) + ' not found!\n')
	else:
		resfile1.write('ISP resources rno=' + str(resources_id) + ' not found!\n')

renameAttach.close()
resfile1.close()
conn.close()
conn2.close()
