# -*-coding:utf-8-*-
"""
@author:Richard
@file: jira_69.py
@time: 2017/11/02
@task: jira-69 http://jira:9002/browse/DATA-69
"""
import datetime
import time

import gevent.monkey
import pymongo

gevent.monkey.patch_socket()
from gevent.pool import Pool

import xxx_diff_process.configSIT as config
import xxx_utils.thirdparty_service as xservice
from xxx_diff_process.untils.ali_opensearch import AliOpenSearch
from xxx_diff_process.untils.common_utils import CommonUtils

# dev mongo iData
mongo_iData_url = u"mongodb://dev:dWVig9ufhNIgImje7eSWJqCCpf%23Y0x@120.27.233.152:10001/iData"
mongo_iData_db = u"iData"

# 初始化mongo_iData
mongo_iData_client = pymongo.MongoClient(host=mongo_iData_url)
mongo_iData_db = mongo_iData_client[mongo_iData_db]


class CourtNotice(object):
    def __init__(self):
        self.update_num = 0
        self.insert_num = 0
        self.pool = Pool(100)
        self.date_format = '%Y%m%d'
        self.DATE_FORMAT = ["%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S",
                            "%Y-%m-%d %H:%M:%S", "%Y%m%d", "%Y%m%d%H%M%S", \
                            "%Y年%m月%d日", "%Y年%m月%d日 %H:%M:%S",
                            "%Y年%m月%d日 %H:%M", \
                            "%Y/%m/%d", "%Y/%m/%d %H:%M:%S", \
                            "%Y.%m.%d", "%Y.%m.%d %H:%M:%S", \
                            "%Y年%m月%d", "%Y年%m月%d %H:%M:%S", \
                            "%b %d %Y", "%b %d %Y %H:%M:%S",
                            "%b %d, %Y %H:%M:%S", "%b %d, %Y %H:%M:%S %p",
                            "%b %d, %Y", "%d-%b-%y"]

    def fix_error_data_mongo_mysql_by_eid(self, eid=None):
        """
        fix court notice error data
        :return:
        """
        # find data by eid
        if eid:
            query_str = {"relate_coms.eid": eid}
        else:
            query_str = {}
        list_e_cn_court_notices = mongo_iData_db["e_cn_court_notices"]
        items = list_e_cn_court_notices.find(query_str)
        print("processing")
        for item in items.batch_size(500):
            # fix date
            item_date = item.get("date")
            item_tribunal = item.get("tribunal")
            date_condition = item_date and not item_date[0].isdigit()
            item_tribunal_condition = item_tribunal and item_tribunal[
                0].isdigit()
            changed = False
            if date_condition and item_tribunal_condition:
                item["date"] = self.format_date(item_tribunal)
                item["tribunal"] = item_date
                changed = True
            # fix url
            item_url = item.get("url")
            if item_url and isinstance(item_url, dict):
                item["url"] = item_url.get("firstSourceText", "")
                changed = True
            if changed:
                # update mongo
                list_e_cn_court_notices.update_one({"_id": item["_id"]},
                                                   {"$set": item})
            item["date"] = self.format_date_all_style(item["date"])
            self.pool.spawn(self.__build_upload, item)
        print("total new insert:%s" % self.insert_num)
        print("total new update:%s" % self.update_num)

    def __build_upload(self, data_instance):
        main_doc = self.__build_sub_table(data_instance)
        for doc in main_doc:
            AliOpenSearch.upload_document(
                config.INTERNAL_SERVICES_URL.mysql_service_url, "gs",
                config.CONFIG.ops_main,
                doc)

    def __build_sub_table(self, data_instance):
        main_table_list = list()
        main_table = dict()
        for temp in data_instance["relate_coms"]:
            relate_com_eid = temp.get("eid")
            if relate_com_eid:
                sql_str = "SELECT p_id, p_type2, p_type3, p_dt1 FROM properties where id='9_{eid}_{p_id}'".format(
                    eid=relate_com_eid, p_id=str(data_instance["_id"]))
                tmp_data = xservice.mysql_service_find(
                    config.INTERNAL_SERVICES_URL.mysql_service_url, u"gs",
                    sql_str, {})
                if tmp_data:
                    p_type2 = tmp_data[0].get("p_type2")
                    p_type3 = tmp_data[0].get("p_type3")
                    p_dt1 = tmp_data[0].get("p_dt1")
                    item_date = data_instance.get("date")
                    if data_instance.get("date"):
                        condition_date = (p_dt1 == item_date)
                    else:
                        condition_date = True
                    all_p_type_exists = (p_type2 == data_instance.get(
                        "case_action", "")) and (p_type3 == data_instance.get(
                        "case_reason", "")) and condition_date
                    if not all_p_type_exists:
                        print(p_dt1, item_date)
                        self.update_num += 1
                        print("updating %s" % self.update_num)
                else:
                    all_p_type_exists = False
                    self.insert_num += 1
                    print("inserting %s" % self.insert_num)
                if not all_p_type_exists:
                    main_table["p_source"] = 9
                    if temp.has_key("eid") and len(temp["eid"]) > 0:
                        if temp.has_key("eid"):
                            main_table["id"] = "9_%s_%s" % (
                                temp["eid"], str(data_instance["_id"]))
                            main_table["p_eid"] = temp["eid"]
                        if temp.has_key("name"):
                            main_table["p_ename"] = str(temp["name"])
                        if data_instance.has_key("_id"):
                            main_table["p_id"] = str(data_instance["_id"])
                        if data_instance.has_key("number"):
                            main_table["p_num1"] = str(data_instance["number"])
                        if data_instance.has_key("type"):
                            main_table["p_type1"] = str(data_instance["type"])
                        if data_instance.has_key("case_action"):
                            main_table["p_type2"] = str(
                                data_instance.get("case_action", ""))
                        if data_instance.has_key("case_reason"):
                            main_table["p_type3"] = str(
                                data_instance.get("case_reason", ""))
                        if data_instance.has_key("court"):
                            main_table["p_court"] = str(data_instance["court"])
                        if data_instance.has_key("title"):
                            main_table["p_title"] = str(data_instance["title"])
                        if data_instance.has_key("tribunal"):
                            main_table["p_dept"] = str(
                                data_instance["tribunal"])
                        if data_instance.has_key("relate_coms"):
                            main_table["p_direct_related"] = ""
                            temp_nm = "eid:"
                            temp_tt = "nm:"
                            temp_sex = "role:"
                            temp_dt = "type:"
                            temp_no = "no:"
                            for temp in data_instance["relate_coms"]:
                                temp_nm = temp_nm + temp["eid"] + ","
                                temp_tt = temp_tt + temp["name"] + ","
                                temp_sex = temp_sex + temp["role"] + ","
                                temp_dt = temp_dt + temp["type"] + ","
                                temp_no += ","
                            temp_nm = temp_nm.rstrip(',') + ";"
                            temp_tt = temp_tt.rstrip(',') + ";"
                            temp_sex = temp_sex.rstrip(',') + ";"
                            temp_dt = temp_dt.rstrip(',') + ";"
                            temp_no = temp_no.rstrip(',') + ";"
                            main_table[
                                "p_direct_related"] = temp_nm + temp_tt + temp_sex + temp_dt + temp_no
                        if data_instance.has_key("url"):
                            main_table["p_source_url"] = str(
                                data_instance["url"])
                        if "date" in data_instance.keys() and len(
                                data_instance["date"]) > 7:
                            main_table["p_dt1"] = CommonUtils.format_date_int(
                                data_instance["date"])
                            main_table["p_yyyy1"] = CommonUtils.format_date_int(
                                data_instance["date"][0:4])
                            main_table[
                                "p_yyyymm1"] = CommonUtils.format_date_int(
                                data_instance["date"][0:7])
                        main_table["p_flag"] = 0
                        main_table_list.append(main_table)
        return main_table_list

    def format_date(self, date_str):
        """
        example: 2016/8/2514:30
        :param date_str:
        :return:
        """
        court_time_range = range(10, 18)
        a = date_str.split('/')
        year = a[0]
        month = int(a[1])
        if month <= 9:
            month = '0' + str(month)
        time_list = a[2].split(':')
        if time_list:
            tmp_len = len(time_list[0])
            if tmp_len == 4:
                day = time_list[0][:2]
                hour = time_list[0][2:]
            elif tmp_len == 2:
                day = '0' + time_list[0][:1]
                hour = '0' + time_list[0][1:]
            elif tmp_len == 3:
                tt = int(time_list[0][1:])
                if tt in court_time_range:
                    day = '0' + time_list[0][0]
                    hour = time_list[0][1:]
                else:
                    day = time_list[0][:2]
                    hour = '0' + time_list[0][2]
            else:
                pass
            minute = time_list[1]
        date_result = "{year}-{month}-{day} {hour}:{minute}:00".format(
            year=year, month=month, day=day, hour=hour, minute=minute)
        return date_result

    def format_date_all_style(self, date_str):
        if date_str is None or len(date_str) == 0:
            return ""
        date_str = date_str.replace("&nbsp;", "").strip()
        if len(date_str) == 0:
            return ""
        result = date_str
        for _dateformat in self.DATE_FORMAT:
            try:
                temp_date = datetime.datetime.strptime(date_str, _dateformat)
                result = temp_date.strftime(self.date_format)
                break
            except:
                result = ""
                continue
        return result


if __name__ == '__main__':
    court_notice = CourtNotice()
    notes = u"""
        1. 存在开庭公告统计数和实际记录数不一致的情况
        2. 开庭公告存在的错误数据需要清洗
        3 .开庭公告有案号的情况下在“”最新风险事件“”处建议把案号显示出来
        4. 开庭入库mysql时间bug，导致云搜索数量不一致的问题清洗
        5. 开庭公告存在重复记录\n
        21. 某个公司开庭公告存在的错误数据需要清洗
        """
    print(notes)
    method_num = raw_input("想处理哪个:")
    if method_num == '21':
        eid = raw_input("输入eid:")
        start = time.time()
        court_notice.fix_error_data_mongo_mysql_by_eid(eid)
        delay = time.time() - start
        print("总耗时:%s 秒" % delay)
    else:
        print(u"暂时不支持此处理")
