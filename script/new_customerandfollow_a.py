#!flask/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import xlsxwriter

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import datetime,time
import pymysql


class OffeceBuilding(object):
    # dbhost = 'rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com'
    # dbuser = 'online'
    # dbpassword = 'online12315'
    dbhost = 'ubanonline.mysql.rds.aliyuncs.com'
    dbuser = 'online'
    dbpassword = 'online12315'
    # dbhost = 'tianjixian.mysql.rds.aliyuncs.com'
    # dbuser = 'zhangjinglei'
    # dbpassword = 'zjl12315'
    dbport = 3306
    dbname = 'corp_officebuilding'
    dbcharset = 'utf8'


class ReportDB(object):
    dbhost = 'rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com'
    dbuser = 'online'
    dbpassword = 'online12315'
    # dbhost = 'tianjixian.mysql.rds.aliyuncs.com'
    # dbuser = 'zhangjinglei'
    # dbpassword = 'zjl12315'
    dbname = 'reportdb'
    dbport = 3306
    dbcharset = 'utf8'


def getToday(days=0):
    # 取当天 getToday()
    # 取昨天 getToday(-1)
    # 取明天 getToday(1)
    d1 = datetime.datetime.now()
    d1 = datetime.datetime(d1.year, d1.month, d1.day, 0, 0, 0)
    if days:
        d1 = d1 + datetime.timedelta(days=days)
    epoch = datetime.datetime(1970, 1, 1, hour=8)
    diff = d1 - epoch
    d = diff.days * 24 * 3600 + diff.seconds
    return d

class ConnectionDB(object):
    def __init__(self, dbuser, dbpassword, dbhost, dbname, dbport, dbcharset):
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.dbhost = dbhost
        self.dbname = dbname
        self.dbport = dbport
        self.dbcharset = dbcharset

    def get_conn(self):
        conn = pymysql.connect(user=self.dbuser, passwd=self.dbpassword, host=self.dbhost, db=self.dbname,
                               charset=self.dbcharset)
        return conn

    def close_conn(self, cursor, conn):
        if not cursor:
            cursor.close()
        if not conn:
            conn.close()


class ReadTable(object):
    def __init__(self):
        ob = OffeceBuilding()
        self.obconn = ConnectionDB(ob.dbuser, ob.dbpassword, ob.dbhost, ob.dbname, ob.dbport, ob.dbcharset)

    def to_kehutongjibiao_area(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            "SELECT c.id, c.customer_name, c.source, c.company, c.position, c.customer_level, dept.dept, dept.city, dept.area, dept.mingroup, c.belonging, c.create_time, ( SELECT t1.create_time FROM ( SELECT ff.customer_id, ff.create_time FROM ( SELECT f.customer_id, f.create_time FROM corp_officebuilding.followlook f ORDER BY f.create_time ) ff GROUP BY ff.customer_id ) t1 WHERE t1.customer_id = cb.customer ) 'lastfollowlooktime', ( SELECT t1.followlook_record FROM ( SELECT ff.customer_id, ff.followlook_record FROM ( SELECT f.customer_id, f.followlook_record FROM corp_officebuilding.followlook f ORDER BY f.create_time ) ff GROUP BY ff.customer_id ) t1 WHERE t1.customer_id = cb.customer ) 'lastfollowlookrecord', cb.belongingid, cb.createid, cb.visit_time, cb.issuccess, cb.reason, cb.remember_uban, cb.remember_gw, cb.gw_contact, cb.remember_house, cb.order_didi, cb.find_office, cb.office_requirement, cb.optimize_service, cb.service_reason FROM corp_officebuilding.callback cb LEFT JOIN ( SELECT IFNULL(dept.dept_name, \"\") AS \"dept\", \"-\" AS \"city\", \"-\" AS \"area\", \"-\" AS \"mingroup\", ui.fullname AS \"customer\", ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name3, \"\"), IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id ) dept ON dept.user_id = cb.belongingid, corp_officebuilding.customer c WHERE c.id = cb.customer and cb.visit_time>=%s and cb.visit_time<%s"
            ,
            [
                st, et
            ]
        )
        print 'select %s' % c
        self.obconn.close_conn(cursor, conn)
        workbook = xlsxwriter.Workbook(filepath + filetime + 'newcustomerandfollow_a.xlsx')
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, unicode('城市', 'utf-8'))
        worksheet.write(0, 1, unicode('大区', 'utf-8'))
        worksheet.write(0, 4, unicode('新增客户总数', 'utf-8'))
        worksheet.write(0, 3, unicode('新增400客户数', 'utf-8'))
        worksheet.write(0, 4, unicode('新增主站注册客户数', 'utf-8'))
        worksheet.write(0, 5, unicode('新增电销客户数', 'utf-8'))
        worksheet.write(0, 6, unicode('新增搜房客户数', 'utf-8'))
        worksheet.write(0, 7, unicode('新增安居客客户数', 'utf-8'))
        worksheet.write(0, 8, unicode('新增58客户数', 'utf-8'))
        worksheet.write(0, 9, unicode('新增赶集客户数', 'utf-8'))
        worksheet.write(0, 10, unicode('新增其他网站客户数', 'utf-8'))
        worksheet.write(0, 11, unicode('新增个人客户数', 'utf-8'))
        worksheet.write(0, 12, unicode('新增其他客户数', 'utf-8'))
        worksheet.write(0, 13, unicode('新增老用户客户数', 'utf-8'))
        worksheet.write(0, 14, unicode('新增带看客户数', 'utf-8'))
        row = 1
        for result in cursor.fetchall():
            for col in xrange(len(result)):
                worksheet.write(row, col, result[col])
            row += 1
        workbook.close()
        filename = filetime + 'newcustomerandfollow_a.xlsx'
        return filename

td = getToday()


class Go(object):
    def set_time(self, st, et):
        print st
        ReadTable().to_kehutongjibiao_area(st, et)
        print "area Ok"

# start = time.time
filetime = sys.argv[3]
filepath = sys.argv[4]
# filepath = "c:/"
# filetime = "2015-10-01--2015-10-30"
if len(sys.argv)>1:
    begin_time = time.mktime(time.strptime(sys.argv[1], "%Y-%m-%d"))
    end_time = time.mktime(time.strptime(sys.argv[2], "%Y-%m-%d"))+24*3600
    Go().set_time(begin_time, end_time)


# Go().set_time('1443628800', '1446134400')
# else:
#     Go().set_time(td,td+24 * 3600)
# end = time.time()
# print end - start


# start = time.time()
# for i in xrange(120):
#    Go().set_time(td-i*24*3600)
# end = time.time()
# print end - start