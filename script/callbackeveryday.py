#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
__author__ = 'xlliu'
import datetime
import pymysql
import time


class OffeceBuilding(object):
    dbhost = 'rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com'
    dbuser = 'online'
    dbpassword = 'online12315'
    # dbhost = 'ubanonline.mysql.rds.aliyuncs.com'
    # dbuser = 'online'
    # dbpassword = 'online12315'
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
    d1 = datetime.datetime(d1.year ,d1.month,d1.day ,0,0,0)
    if days:
        d1 = d1 + datetime.timedelta(days = days)
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
        rd = ReportDB()
        self.obconn = ConnectionDB(ob.dbuser, ob.dbpassword, ob.dbhost, ob.dbname, ob.dbport, ob.dbcharset)
        self.rdconn = ConnectionDB(rd.dbuser, rd.dbpassword, rd.dbhost, rd.dbname, rd.dbport, rd.dbcharset)
    # 用房日期    客户意向
    def to_kehuzhuangtai(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            "SELECT c.id, c.customer_name, c.source, c.company, c.position, c.customer_level, dept.dept, dept.city, dept.area, dept.mingroup, c.belonging, c.create_time, ( SELECT t1.create_time FROM ( SELECT ff.customer_id, ff.create_time FROM ( SELECT f.customer_id, f.create_time FROM corp_officebuilding.followlook f ORDER BY f.create_time ) ff GROUP BY ff.customer_id ) t1 WHERE t1.customer_id = cb.customer ) 'lastfollowlooktime', ( SELECT t1.followlook_record FROM ( SELECT ff.customer_id, ff.followlook_record FROM ( SELECT f.customer_id, f.followlook_record FROM corp_officebuilding.followlook f ORDER BY f.create_time ) ff GROUP BY ff.customer_id ) t1 WHERE t1.customer_id = cb.customer ) 'lastfollowlookrecord', cb.belongingid, cb.create_name, cb.visit_time, cb.issuccess, cb.reason, cb.remember_uban, cb.remember_gw, cb.gw_contact, cb.remember_house, cb.order_didi, cb.find_office, cb.office_requirement, cb.optimize_service, cb.service_reason FROM corp_officebuilding.callback cb LEFT JOIN ( SELECT IFNULL(dept.dept_name, \"\") AS \"dept\", \"-\" AS \"city\", \"-\" AS \"area\", \"-\" AS \"mingroup\", ui.fullname AS \"customer\", ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name3, \"\"), IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id ) dept ON dept.user_id = cb.belongingid, corp_officebuilding.customer c WHERE c.id = cb.customer and cb.visit_time>=%s and cb.visit_time<%s"
            ,
            [
                st, et
            ]
        )
        print 'select %s' % c
        self.obconn.close_conn(cursor, conn)
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            for result in cursor.fetchall():
                l = list(result)+[st]
                if str(l[2])=="1":
                    l[2] = u"个人用户"
                elif str(l[2])=="2":
                    l[2] = u"400电话"
                elif str(l[2])=="3":
                    l[2] = u"主站"
                elif str(l[2])=="4":
                    l[2] = u"赶集"
                elif str(l[2])=="5":
                    l[2] = u"58同城"
                elif str(l[2])=="6":
                    l[2] = u"安居客"
                elif str(l[2])=="7":
                    l[2] = u"搜房帮"
                elif str(l[2])=="8":
                    l[2] = u"其他网站采集"
                elif str(l[2])=="9":
                    l[2] = u"老用户"
                elif str(l[2])=="9":
                    l[2] = u"公众号"
                elif str(l[2])=="11":
                    l[2] = u"电话销售"
                else:
                    l[2] = u"其他来源"
                if l[11]:
                    l[11] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(l[11]))
                if l[12]:
                    l[12] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(l[12]))
                if l[16]:
                    l[16] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(l[16]))
                for num in xrange(17, 29):
                    if num ==17:
                        if l[17] == 1:
                            l[17] = '全部'
                        if l[17] == 2:
                            l[17] = '成功'
                        if l[17] == 3:
                            l[17] = '不成功'
                    if num == 18:
                        if l[18] == 1:
                            l[18] = '电话号码不正确'
                        if l[18] == 2:
                            l[18] = '客户挂断'
                        if l[18] == 3:
                            l[18] = '客户未接听'
                        if l[18] == 0:
                            l[18] = ''
                    if num in (19, 20, 21, 22, 23, 24, 25, 26, 27):
                        if l[num]:
                            l[num] = '是'
                        else:
                            l[num] = '否'

                # print l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11]
                cursorrd.execute(
                    'insert into callbackeveryday(customer_id,customer_name,source,company,position,'
                    'customer_level,dept,city,area,mingroup,'
                    'belonging,create_time,lastfollowlooktime,lastfollowlookrecord,belongingid,'
                    'create_name,visit_time,issuccess,reason,remember_uban,'
                    'remember_gw,gw_contact,remember_house,order_didi,find_office,'
                    'office_requirement,optimize_service,service_reason,date) '
                    'values(%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s)'
                    , [l[i] for i in xrange(29)]
                )
            connrd.commit()
        except Exception, e:
            print 'add false', e
            connrd.rollback()
        finally:
            self.rdconn.close_conn(cursorrd, connrd)

    def check_Kehuzhuangtai(self, d):
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        d = cursorrd.execute(
            'delete from callbackeveryday where date=%s', [d]
        )
        print 'delete %s' % d
        connrd.commit()
        self.rdconn.close_conn(cursorrd, connrd)


yd = getToday(-1)


class Go(object):

    def set_time(self, st=yd):
        print st
        et = st +24*3600
        self.clearTable(st)
        ReadTable().to_kehuzhuangtai(st, et)

    def clearTable(self, st=yd):
        ReadTable().check_Kehuzhuangtai(st)


Go().set_time()
print 'ok'

# for i in xrange(7):
#    Go().set_time(yd-i*24*3600)
