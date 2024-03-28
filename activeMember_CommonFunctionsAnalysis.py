#!/usr/bin/python3.8
# -*- coding: utf-8 -*-

import datetime
import pymysql
import sys
from pymongo import MongoClient
from dateutil.relativedelta import relativedelta


sys.path.append("..")
from settings import *


conn = pymysql.connect(host=mysqlRead_hostname, database=mysqlRead_dbname, user=mysqlRead_user, passwd=mysqlRead_password, charset='utf8')
cursor1 = conn.cursor(cursor=pymysql.cursors.DictCursor)

client = MongoClient(mongo_hostname, monggo_port)
db_edumarket = client[mongo_dbname_edumarket]
db_marketanalysis = client[mongo_dbname_marketanalysis]
member_growing_col = db_marketanalysis.member_growing
analysis_functions_col = db_marketanalysis.activeMember_CommonFunctionsAnalysis

#分析活躍性會員取得會員id
nowdate = datetime.datetime.now()
beforeOneYearDate = nowdate + relativedelta(years=-1)

heavyUserList = db_edumarket.educloud_loginhistory.aggregate(
   [
      {
        '$match' : {
           'created_at':{'$gte':beforeOneYearDate, '$lt':nowdate}
        },
      },
      {
        '$group' : {
           '_id' : { 'UserId': "$UserAccount" },
           'count': { '$sum': 1 }
        }
      },
      { '$sort' : { 'count' : -1 } }
   ]
)

#一年登入超過三次就算在活躍性成員中
i =0
active_user = []
print(heavyUserList)
for doc in list(heavyUserList) :
     if (doc['count'] > 3) :
         i = i + 1
         member_dict = doc['_id']
         if (member_dict['UserId'] != '') :
            active_user.append(member_dict['UserId'])

#從mysql取得活躍性會員的member_id
print('----------')
print('從mysql取得會員member_id')
active_member_id = []
print(active_user)

for user_id in active_user :
    SQL1 = "select id from member where userid like '%" + user_id + "'"
    cursor1.execute(SQL1)
    member_list = cursor1.fetchall()
    for member_id in member_list :
        print(member_id.get('id'))
        active_member_id.append(str(member_id.get('id')))

#利用member_id查詢操作行為
category_dict = {}
category_dict['category_download_file'] = ('下載檔案',0)
category_dict['category_post_comment'] = ('發表評論',0)
category_dict['category_click_like'] = ('按讚',0)
category_dict['category_report'] = ('檢舉',0)
category_dict['category_click_outsidelink'] = ('點擊外部連結',0)
category_dict['category_click_resource'] = ('點擊教學資源',0)
category_dict['category_search_subject'] = ('檢索查詢-學科',0)
category_dict['category_search_keyword'] = ('檢索查詢-關鍵字',0)
category_dict['category_search_people'] = ('檢索查詢-作者/提供者/推薦者',0)
category_dict['category_share_resource'] = ('分享教學資源',0)
category_dict['category_browse_resource'] = ('瀏覽相關資源',0)
category_dict['category_switch_device'] = ('切換網頁/行動版',0)
category_dict['category_operate_resource'] = ('操作資源管理(我的資源)',0)
category_dict['category_enter_membercenter'] = ('會員中心',0)
category_dict['category_batchupload'] = ('批次上傳資源',0)
category_dict['category_apply_partner'] = ('申請成為加盟單位',0)
category_dict['category_personal_searchsetting'] = ('個人化檢索設定',0)
category_dict['category_review_progress'] = ('審查進度查詢',0)
category_dict['category_report_progress'] = ('檢舉進度查詢',0)
category_dict['category_api_service'] = ('API應用服務',0)
category_dict['category_notification_message'] = ('通知訊息',0)
category_dict['category_use_resource'] = ('資源使用情形',0)
category_dict['category_operation_history'] = ('操作歷程',0)
category_dict['category_co-writing_resource'] = ('資源共筆操作',0)
category_dict['category_co-writing_browser'] = ('瀏覽資源的共筆記錄',0)
category_dict['category_trace_resource'] = ('追蹤資源',0)
category_dict['category_cancel_trace_resource'] = ('取消追蹤資源',0)
category_dict['category_previwe_file'] = ('檔案預覽',0)
category_dict['category_favorite_resource'] = ('收藏引用資源',0)
category_dict['category_edit_member'] = ('修改會員資料',0)


#統整class_code到設置的類別中
print('------------------')
print('統整class_code')
for member_id in active_member_id :
    print(member_id)
    activeUserList = db_marketanalysis.member_behaviorlog.find({"member_id" : int(member_id) , "eventtime": {"$gte": beforeOneYearDate, "$lt": nowdate}}, {"class_code" : 1})

    for doc in activeUserList :
        code = str(doc.get('class_code'))
        if (code == 'F') or (code == 'M_F'):
            category_dict['category_download_file'] = '下載檔案', int(category_dict['category_download_file'][1]) + 1
        elif (code == 'C') or (code == 'M_C'):
            category_dict['category_post_comment'] = '發表評論', int(category_dict['category_post_comment'][1]) + 1
        elif (code == 'MC') or (code == 'M_MC'):
            category_dict['category_click_like'] = '按讚', int(category_dict['category_click_like'][1]) + 1
        elif (code == 'H') or (code == 'M_H'):
            category_dict['category_report'] = '檢舉', int(category_dict['category_report'][1]) + 1
        elif (code == 'L') or (code == 'M_L') or (code == 'M_L') or (code == 'EPedia_L') or (code == 'EVideo_L') or (
                code == 'TC_L'):
            category_dict['category_click_outsidelink'] = '點擊外部連結', int(
                category_dict['category_click_outsidelink'][1]) + 1
        elif (code == 'D') or (code == 'M_D'):
            category_dict['category_search_subject'] = '檢索查詢-學科', int(category_dict['category_search_subject'][1]) + 1
        elif (code == 'Q') or (code == 'QSK') or (code == 'QA') or (code == 'QA_K9') or (code == 'QA_K12') or (
                code == 'Q_E') or (code == 'Q_J') or (code == 'Q_S') or (code == 'Q_V') or (code == 'Q_Web') or (
                code == 'Q_Ebook') \
                or (code == 'M_Q') or (code == 'M_Q_K9') or (code == 'M_Q_K12') or (code == 'M_Q_Sup') or (
                code == 'IK') or (code == 'K') or (code == 'RK') or (code == 'DK') or (code == 'M_RK') \
                or (code == 'HK'):
            category_dict['category_search_keyword'] = '檢索查詢-關鍵字', int(category_dict['category_search_keyword'][1]) + 1
        elif (code == 'AU') or (code == 'M_AU') or (code == 'UP') or (code == 'M_UP'):
            category_dict['category_search_people'] = '檢索查詢-作者/提供者/推薦者', int(
                category_dict['category_search_people'][1]) + 1
        elif (code == 'SE') or (code == 'M_SE') or (code == 'SG') or (code == 'M_SG') or (code == 'SF') or (
                code == 'M_SF') or (code == 'ST') \
                or (code == 'M_ST') or (code == 'SL') or (code == 'M_SL'):
            category_dict['category_share_resource'] = '分享教學資源', int(category_dict['category_share_resource'][1]) + 1
        elif (code == 'M_Relation') or (code == 'Relation') or (code == 'RelationMath'):
            category_dict['category_browse_resource'] = '瀏覽相關資源', int(category_dict['category_browse_resource'][1]) + 1
        elif (code == 'Switch') or (code == 'M_Switch'):
            category_dict['category_switch_device'] = '切換網頁/行動版', int(category_dict['category_switch_device'][1]) + 1
        elif (code == 'MYRES_AFol') or (code == 'MYRES_AFolF') or (code == 'MYRES_DelFol') or (
                code == 'MYRES_DelFolF') or (code == 'MYRES_RFol') or (code == 'MYRES_RfolF') or (
                code == 'MYRES_DownFile') or (code == 'MYRES_DownFileF') \
                or (code == 'MYRES_DownLink') or (code == 'MYRES_DelFile') or (code == 'MYRES_DelFileF') or (
                code == 'MYRES_DelR') or (code == 'MYRES_DelRF') or (code == 'MYRES_SR') or (code == 'MYRES_SRF') or (
                code == 'MYRES_UP') \
                or (code == 'MYRES_UPF') or (code == 'MYRES_MVFile') or (code == 'MYRES_MVFileF') or (
                code == 'MYRES_MVRes') or (code == 'MYRES_MVResF') or (code == 'MYRES_CFol') or (
                code == 'MYRES_CFolNon') or (code == 'MYRES_CRes') \
                or (code == 'MYRES_MVFol') or (code == 'MYRES_MVFolF') or (code == 'MYRES_AResK9') or (
                code == 'MYRES_AResK9F') or (code == 'MYRES_EResK9') or (code == 'MYRES_EResK9F') or (
                code == 'MYRES_AResK12') \
                or (code == 'MYRES_AResK12F') or (code == 'MYRES_EResK12') or (code == 'MYRES_EResK12F') or (
                code == 'MYRES_AResEb') or (code == 'MYRES_AResEbF') or (code == 'MYRES_EResEb') or (
                code == 'MYRES_EResEbF') \
                or (code == 'MYRES_UploadFileTmp') or (code == 'MYRES_AResK9DecideAddURL') or (
                code == 'MYRES_AResK9DecideAddFile') or (code == 'MYRES_AResK12DecideAddURL') or (
                code == 'MYRES_AResK12DecideAddFile') or (code == 'MYRES_AResEBookDecideAddURL') or (
                code == 'MYRES_AResEBookDecideAddFile') \
                or (code == 'MYRES_AResAppDecideAddURL') or (code == 'MYRES_AResAppDecideAddFile') or (
                code == 'MYRES_EResK9DecideAddURL') or (code == 'MYRES_EResK9DecideAddFile') or (
                code == 'MYRES_EResK12DecideAddURL') or (code == 'MYRES_EResK12DecideAddFile') or (
                code == 'MYRES_EResEBookDecideAddURL') \
                or (code == 'MYRES_EResEBookDecideAddFile') or (code == 'MYRES_EResAPPDecideAddURL') or (
                code == 'MYRES_EResAppDecideAddFile') or (code == 'MYRES_MemberByMillionSec') or (
                code == 'MYRES_ARLink'):
            category_dict['category_operate_resource'] = '操作資源管理(我的資源)', int(
                category_dict['category_operate_resource'][1]) + 1
        elif (code == 'Member_Center'):
            category_dict['category_enter_membercenter'] = '會員中心', int(
                category_dict['category_enter_membercenter'][1]) + 1
        elif (code == 'BatchUploadFileSuccess') or (code == 'BatchUploadFileFail'):
            category_dict['category_batchupload'] = '批次上傳資源', int(category_dict['category_batchupload'][1]) + 1
        elif (code == 'ApplyToPartnerSuccess') or (code == 'ApplyToPartnerFail'):
            category_dict['category_apply_partner'] = '申請成為加盟單位', int(category_dict['category_apply_partner'][1]) + 1
        elif (code == 'SearchSetting'):
            category_dict['category_personal_searchsetting'] = '個人化檢索設定', int(
                category_dict['category_personal_searchsetting'][1]) + 1
        elif (code == 'QueryRES_CensorshipProgress') or (code == 'QueryRES_CensorshipProgressDetail'):
            category_dict['category_review_progress'] = '審查進度查詢', int(category_dict['category_review_progress'][1]) + 1
        elif (code == 'QueryRES_Harmful') or (code == 'QueryRES_HarmfulDetail') or (
                code == 'QueryRES_HarmfulCancelSuccess') or (code == 'QueryRES_HarmfulCancelFail'):
            category_dict['category_report_progress'] = '檢舉進度查詢', int(category_dict['category_report_progress'][1]) + 1
        elif (code == 'Member_AddApiSuccess') or (code == 'Member_AddApiFail') or (
                code == 'Member_DeleteApiSuccess') or (code == 'Member_DeleteApiFail') or (
                code == 'Member_ModifyApiSuccess') or (code == 'Member_ModifyApiFail'):
            category_dict['category_api_service'] = 'API應用服務', int(category_dict['category_api_service'][1]) + 1
        elif (code == 'NoticeList') or (code == 'NoticeDetail'):
            category_dict['category_notification_message'] = '通知訊息', int(
                category_dict['category_notification_message'][1]) + 1
        elif (code == 'ResourceUsage') or (code == 'ResourceUsage_export_Excel') or (
                code == 'ResourceUsage_FileDetail') or (code == 'ResourceUsage_FileDetailClickURL') or (
                code == 'ResourceUsage_FileDetailDownLoad'):
            category_dict['category_use_resource'] = '資源使用情形', int(category_dict['category_use_resource'][1]) + 1
        elif (code == 'Member_History') or (code == 'Member_HistoryDetail'):
            category_dict['category_operation_history'] = '操作歷程', int(
                category_dict['category_operation_history'][1]) + 1
        elif (code == 'CollaborativeEdit') or (code == 'CollaborativeEdit_Cancel') or (
                code == 'CollaborativeEdit_UpdateSuccess'):
            category_dict['category_co-writing_resource'] = '資源共筆操作', int(
                category_dict['category_co-writing_resource'][1]) + 1
        elif (code == 'CollaborativeEdit_History'):
            category_dict['category_co-writing_browser'] = '瀏覽資源的共筆記錄', int(
                category_dict['category_co-writing_browser'][1]) + 1
        elif (code == 'Resource_Follow'):
            category_dict['category_trace_resource'] = '追蹤資源', int(category_dict['category_trace_resource'][1]) + 1
        elif (code == 'Resource_FollowCancel'):
            category_dict['category_cancel_trace_resource'] = '取消追蹤資源', int(
                category_dict['category_cancel_trace_resource'][1]) + 1
        elif (code == 'FilePreview'):
            category_dict['category_previwe_file'] = '檔案預覽', int(category_dict['category_previwe_file'][1]) + 1
        elif (code == 'MYRES_ALink') or (code == 'MYRES_ALinkNon') or (code == 'MYRES_ALinkF') or (
                code == 'MYRES_ARLinkNon') or (code == 'MYRES_ARLinkF') \
                or (code == 'MYRES_ShaFol') or (code == 'MYRES_QuoFol') or (code == 'MYRES_QuoFolF'):
            category_dict['category_favorite_resource'] = '收藏引用資源', int(
                category_dict['category_favorite_resource'][1]) + 1
        elif (code == 'Member_UpdateSuccess') or (code == 'Member_UpdateFail') or (
                code == 'Member_UpdateContactSuccess') or (code == 'Member_UpdateContactFail') or (
                code == 'Member_UpdateGenderSuccess') \
                or (code == 'Member_UpdateGenderFail') or (code == 'Member_UpdateGenderSuccess') or (
                code == 'Member_UpdateGenderFail'):
            category_dict['category_edit_member'] = '修改會員資料', int(category_dict['category_edit_member'][1]) + 1
        elif (code == 'R') or (code == 'M_R'):
            category_dict['category_click_resource'] = '點擊教學資源', int(category_dict['category_click_resource'][1]) + 1

used_function_list = []
for key in category_dict :
    category_dict_list = {}
    category_dict_list['category_name'] = category_dict[key][0]
    category_dict_list['count'] = category_dict[key][1]
    used_function_list.append(category_dict_list)
print(used_function_list)

for doc in used_function_list :
    myquery = {'category_name' : doc.get('category_name')}
    newvalues = { "$set": { "category_name": str(doc.get('category_name')), 'count' : int(doc.get('count'))}}
    analysis_functions_col.update_one(myquery, newvalues, upsert=True)

conn.close()
client.close()
