#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：get_ebook_thumbnail.py
#功能說明：抓取教育大市集之電子書縮圖(圓夢繪本系列)，存入固定路徑下
#輸入參數：無
#資料來源：edumarket_latest -> resources
#輸出結果：縮圖存放路徑

import os, sys
import pymysql, requests
from bs4 import BeautifulSoup
from PIL import Image

sys.path.append("..")
from settings import *


def insert_str(string, str_to_insert, index):
    return string[:index] + str_to_insert + string[index:]


conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

SQL = 'select * from resources where bmtype="ebook" and member_id=160044'
cursor.execute(SQL)
imageBase_url = 'https://market.cloud.edu.tw/image/'
resBase_url = 'https://market.cloud.edu.tw/resources/ebook/'
storyBase_url = 'https://storybook.nlpi.edu.tw/'
playbookBase_url = 'https://storybook.nlpi.edu.tw/upload/content/'
blankPic_list = [1805946, 1805947, 1805938]

ebook_datas = cursor.fetchall()
for ebook_data in ebook_datas:
	ebook_id = ebook_data['id']
	thumbnail_url = imageBase_url + str(ebook_id)
	thumbnail_content = requests.get(thumbnail_url).text

	## 篩選無縮圖之電子書
	if len(thumbnail_content) == 0 or ebook_id in blankPic_list:
		print(ebook_id)
		resource_url = resBase_url + str(ebook_id)
		web_content = requests.get(resource_url).text
		soup = BeautifulSoup(web_content, 'html.parser')
		
		thumbnail = soup.find_all('a', class_='link')
		if len(thumbnail) > 0:
			story_url = str(thumbnail[0])
			story_url = story_url.replace('<a class="link" href="#" title="請先登入，登入後按下連結連至外部網站顯示網頁">', '')
			story_url = story_url.replace('</a>', '')
			if ebook_id == 1805826:
				story_url = 'https://storybook.nlpi.edu.tw/book-single.aspx?BookNO=1080'
			
			storyImg_url = ''
			if 'book-single.aspx' in story_url:
				story_content = requests.get(story_url).text
				soup = BeautifulSoup(story_content, 'html.parser')
				img_str = soup.find_all('img', class_='book-card-img-pd')
				img_str = str(img_str[0])
				img_str = img_str.replace('<img class="book-card-img-pd" src="', '')
				img_str = img_str.replace('"/>', '')
				storyImg_url = storyBase_url + img_str

			elif 'playbook2.aspx' in story_url:
				storyImg_url = playbookBase_url + story_url[-4:] + '/' + story_url[-4:] + '_P01.jpg'
		
			response = requests.get(storyImg_url)
			img_path = hex(ebook_id).replace('0x', '00')
			img_path = insert_str(img_path, '/', 2)
			img_path = insert_str(img_path, '/', 5)
			img_path = insert_str(img_path, '/', 8)
			# folder_path = os.path.join('C:/Users/derek/OneDrive/Desktop/python/', img_path[:8])
			folder_path = os.path.join('/www/edudata/thumbnail/', img_path[:8])

			if os.path.exists(folder_path) == False:
				os.makedirs(folder_path)

			img_path = folder_path + img_path[8:] + '_thumb.png' 
			file = open(img_path, "wb")
			file.write(response.content)
			file.close()
			
			print(img_path)
			img = Image.open(img_path)
			img = img.resize( (300,170), Image.ANTIALIAS) #指定長與寬並進行縮圖製作
			img.save(img_path)