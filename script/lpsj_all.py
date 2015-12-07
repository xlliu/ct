#!flask/bin/python
# -*- coding: utf-8 -*-
import sys

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
    # dbhost = 'rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com'
    # dbuser = 'online'
    # dbpassword = 'online12315'
    dbhost = 'tianjixian.mysql.rds.aliyuncs.com'
    dbuser = 'zhangjinglei'
    dbpassword = 'zjl12315'
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
        rd = ReportDB()
        self.obconn = ConnectionDB(ob.dbuser, ob.dbpassword, ob.dbhost, ob.dbname, ob.dbport, ob.dbcharset)
        self.rdconn = ConnectionDB(rd.dbuser, rd.dbpassword, rd.dbhost, rd.dbname, rd.dbport, rd.dbcharset)

    def to_kehutongjibiao(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            "SELECT t1.dept, t1.city, t1.area, t1.mingroup, t1.customer, IFNULL(t2.all_num, 0), IFNULL(t3.400_num, 0), IFNULL(t4.zhuzhan, 0), IFNULL(t5.dianxiao, 0), IFNULL(t6.sofang, 0), IFNULL(t7.anjuke, 0), IFNULL(t8.58_tongcheng, 0), IFNULL(t9.ganji, 0), IFNULL(t10.qita, 0), IFNULL(t11.geren, 0), IFNULL(t12.daikan, 0), IFNULL(t13.qita_all, 0), IFNULL(t14.laoyonghu, 0) FROM ( SELECT IFNULL(dept.dept_name, \"\") AS \"dept\", \"-\" AS \"city\", \"-\" AS \"area\", \"-\" AS \"mingroup\", ui.fullname AS \"customer\", ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name3, \"\"), IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id ) t1 LEFT JOIN ( SELECT belongingid, count(phone_num) all_num FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s )) t GROUP BY belongingid ) t2 ON t2.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) 400_num FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 2 ) t GROUP BY belongingid ) t3 ON t3.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) zhuzhan FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 3 ) t GROUP BY belongingid ) t4 ON t4.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) dianxiao FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 11 ) t GROUP BY belongingid ) t5 ON t5.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) sofang FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 7 ) t GROUP BY belongingid ) t6 ON t6.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) anjuke FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 6 ) t GROUP BY belongingid ) t7 ON t7.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) 58_tongcheng FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 5 ) t GROUP BY belongingid ) t8 ON t8.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) ganji FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 4 ) t GROUP BY belongingid ) t9 ON t9.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) qita FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 8 ) t GROUP BY belongingid ) t10 ON t10.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) geren FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 1 ) t GROUP BY belongingid ) t11 ON t11.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) qita_all FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 0 ) t GROUP BY belongingid ) t13 ON t13.belongingid = t1.user_id LEFT JOIN ( SELECT belongingid, count(phone_num) laoyonghu FROM ( SELECT DISTINCT t1.belongingid, t1.phone_num FROM ( SELECT * FROM corp_officebuilding.customer c WHERE c.create_time >=% s AND c.create_time <% s ) t1 WHERE t1.phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time <% s ) AND source = 9 ) t GROUP BY belongingid ) t14 ON t14.belongingid = t1.user_id LEFT JOIN ( SELECT follow_id, follow_man, COUNT(id) daikan FROM corp_officebuilding.followlook f WHERE followlook_okdate >= % s AND followlook_okdate < % s AND customer_id IN ( SELECT id FROM customer WHERE create_time >= % s AND create_time < % s AND phone_num NOT IN ( SELECT c.phone_num FROM corp_officebuilding.customer c WHERE c.create_time < % s )) GROUP BY follow_id, follow_man ) t12 ON t12.follow_id = t1.user_id"
            ,
            [
                st, et, st, st, et, st,
                st, et, st, st, et, st,
                st, et, st, st, et, st,
                st, et, st, st, et, st,
                st, et, st, st, et, st,
                st, et, st, st, et, st,
                st, et, st, et, st
            ]
        )
        print 'select %s' % c
        self.obconn.close_conn(cursor, conn)
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            i = 0
            for result in cursor.fetchall():
                l = tuple(list(result) + [st])
                # print i, l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14], l[15], l[16], l[17], l[18]
                cursorrd.execute(
                    'insert into new_customerandfollow(dept, city, area, mingroup, customer, all_num, 400_num, zhuzhan, dianxiao, sofang, anjuke, 58_tongcheng, ganji, qita, geren, daikan, qita_all, laoyonghu,date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14], l[15], l[16], l[17], l[18]]
                )
                i+=1
            connrd.commit()
        except Exception, e:
            print 'add false', e
            connrd.rollback()
        finally:
            self.rdconn.close_conn(cursorrd, connrd)

    def check_kehutongjibiao(self, d):
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        d = cursorrd.execute(
            'delete from new_customerandfollow where date=%s', [d]
        )
        print 'delete %s' % d
        connrd.commit()
        self.rdconn.close_conn(cursorrd, connrd)


td = getToday()


class Go(object):
    def set_time(self, st=td):
        print st
        et = st + 24 * 3600
        self.clearTable(st)
        ReadTable().to_kehutongjibiao(st-20*24*3600, et)

    def clearTable(self, st=td):
        ReadTable().check_kehutongjibiao(st)


# start = time.time()
Go().set_time()
# end = time.time()
# print end - start


# start = time.time()
# for i in xrange(120):
#    Go().set_time(td-i*24*3600)
# end = time.time()
# print end - start
