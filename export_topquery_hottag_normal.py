#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#
# 從Google Seach Console中的熱門查詢詞,取得前20筆熱門的查詢詞以產生供
# 前台標門標籤(hottag_normal2.html)使用的網頁資料.
#
from pymongo import MongoClient
import sys
sys.path.append("..")
from settings import *

# Declare mongo database variables
client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]


hotkeyword_header = """
                        <div class="c-block pt30">
                            <div class="title pt5">
                                <h5>熱門標籤</h5>
                                <div class="title-border"><div class="colorbar-s"></div></div>
                            </div>
                            <div class="taglist" id="tags">
"""
hotkeyword_footer = """
                            </div>
                        </div>
"""

file = open('/www/edudata/staticpage/hottag_normal2.html', 'w')
topQyery = db_edumarket.google_top_query.find({'isdelete':'N'}).sort([('clicks',-1)]).limit(20)
if (len(list(topQyery)) > 0) :
	file.write(hotkeyword_header)
	for row in topQyery :
		file.write('    <a target="_blank" href="/search/search.jsp?q={}&sp=p0" onmousedown="mevent(\'HK\',\'{}\')">{}</a>'.format(row['keyword'], row['keyword'], row['keyword']))
	file.write(hotkeyword_footer)
file.close()
client.close()
