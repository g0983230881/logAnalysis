# MySQL 讀寫分離

將MySQL僅有讀取的SQL操作分離，Repository裡的Python檔案已完成分離。  
目前已修改的檔案紀錄於google excel:  
https://docs.google.com/spreadsheets/d/1aZEOqvUtguutycSlVQrpWEgbElrCoYzm3HQLNeHEsaE/edit?usp=sharing  

## 05/09 下方待確認檔案與問題

#### export_topquery_hottag_normal.py  
權限不足	Permission denied: '/www/edudata/staticpage/hottag_normal2.html'	sudo python3.8 export_topquery_hottag_normal.py 才行  

#### search_analytics_top_query.py  
要求輸入驗證碼, 但不知道驗證碼是什麼, 之前詢問過這隻程式怎麼處置, 結論是不用  
Go to the following link in your browser:  
https://accounts.google.com/o/oauth2/auth?client_id=41540491768-jcm7sla77f58bchh86jiqkr93l5cn7dq.apps.googleusercontent.com&redirect_uri=urn%3Aietf%3Awg%3Aoauth%3A2.0%3Aoob&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fwebmasters.readonly&access_type=offline&response_type=code  
點進連結後會顯示已封鎖存取權, 錯誤代碼 400： invalid_request  
Enter verification code:  

#### get_ebook_thumbnail.py	權限寫入問題	
PermissionError: [Errno 13] Permission denied: '/www/edudata/thumbnail/00/1b/8e/72_thumb.png'  

#### sync_resource_link.py	
會搜尋一個不存在的collection(portal),	印象中這個在之前討論時結論是不用  
