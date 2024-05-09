Log 人次/使用量統計程式說明
--------------------

主要說明:
  settings.json需視情況調整，有settings.readme.json參考

  searchlog目前的放置方式為
  searchlog  # 這個路徑寫在import_search_log.py 中的第10行，需要改
  
  所有的log檔都必須放在search_log_dir變數內 (保留searchlog_hostip_date.txt格式)

--------------------------------------------------------------------------------------

# import member (directory: importlog)

指令:
  python import_mysql_to_mongo.py member

說明:
  此步是將mysql db中的member匯入到mongodb中，讓後面的程式可以使用 (主要需要member.idtype, member.city_id)

--------------------------------------------------------------------------------------
# import search log (directory: importlog)

指令:
  python import_search_log.py 192.168.80.5 2016-05-20 2016-05-26 (此範例將匯入5/20-5/25的search log)
  python import_search_log.py 192.168.80.5 2016-05-20 (此範例為匯入5/20當日的search log)

說明:
  需依照第一部份的檔案結構，且修改第10行的路徑到search log root位置

-- search_log範例

{
  "_id" : ObjectId("57464272e5be3a6e4237fa37"),   # 自動產生
  "since" : ISODate("2014-10-16T15:58:31.281Z"),  # Log產生時間Index
  "user_id" : -1,                                 # 使用者 (guest為-1)
  "from_ip" : "140.109.16.137",                   # 來源 IP
  "class_code" : "Q",                             # 行為代碼
  "main_col" : "",                                # 主欄位
  "sec_col" : "台北",                              # 次欄位
  "log_host" : "192.168.80.5",                    # Server IP
  "session_id" : "82D0A92A69E62E7CCE001536A21F7D6D",
  "cookie_iden" : "F3PMi7nhUV"
}



--------------------------------------------------------------------------------------

# intermediate colletion (directory: importlog)
指令: 
  $>_ bash inter_collect.sh

說明:
  construct_intermedia_collection.py [date] [mode]
  * date為yyyy-mm-dd格式之日期
  * mode可為 
    - s: Session mode
    - c: Cookie mode
    - r: Resources mode [此次未用到]

--------------------------------------------------------------------------------------

# traffic for session/usage (directory: traffic)
指令:
  $>_ bash run_traffic_statistic.sh

說明:
1.
  init_traffic_documents.py [date] [mode]
  建立初始化(統計資訊為0)的documents
  * date為yyyy-mm或yyyy-mm-dd格式，依mode決定
  * mode可為
    - m: Month mode
    - d: Day mode

2.
  city_based_traffic_session.py [date]
  city_based_traffic_usage.py [date]
  分別用於統計人次與使用量

  生成的collections有
    * city_traffic_month
    * city_traffic_day
    * traffic_month
    * traffic_day
  欄位說明
  period: 時間區段 (分別有yyyy-mm, yyyy-mm-dd格式）
  city_id: 縣市ID (city_開頭的)
  rank: 定義city的排序順序，便於API做sort (由init_traffic_document定義)

  t_開頭欄位格式為:
    t_[isp|openid|social|guest]_[all|c1|c2|c3|c4|c5|c6|session]
    isp, openid...為帳戶類型，此版本加入guest統計非登入使用資訊
    all, c1~6, session為全部使用量，分類使用量(六類)，人次


--------------------------------------------------
此三項資料即是EduSSO(OpenID), ISP 及 未登入 (guest) 的使用人次
db.traffic_month.find({period:'2016-07'}, {t_openid_session:1, t_isp_session: 1, t_guest_session: 1}).pretty() 

--------------------------------------------------
統計使用率前10名的教學資源名單 (directory: traffic)
python top10_resources.py 2016-07-01 2016-09-30

