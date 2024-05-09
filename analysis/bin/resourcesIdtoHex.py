#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
#程式名稱：
#功能說明：匯整教學資源使用量
#輸入參數：無
#資料來源：
#輸出結果：
#開發人員：Chi-Wen Fann


def resourcesIdToPath(resources_id):
	resourcesId = str(hex(resources_id))[2:]
	resourcesIdhex = resourcesId.rjust(8, '0')
	path = ''
	for i in range(0, len(resourcesIdhex), 2) :
		path += '/' + resourcesIdhex[i:i+2]
	return path

print(resourcesIdToPath(100))
