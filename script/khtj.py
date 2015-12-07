#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import datetime
import pymysql


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

    def to_kehutongjibiao(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            "SELECT u.fullname, IFNULL(tx.cre, 0), IFNULL(txv.cre, 0), IFNULL(txtoday.cre, 0)-IFNULL(tx.cre, 0),'--','--','--', IFNULL(dept.dept_name,0) "
            "FROM ( SELECT user_id, fullname,dept_id, city_id FROM corp_management. USER cu WHERE cu.dept_id=13) u "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tt GROUP BY tt.create_id ) tx ON tx.create_id = u.user_id "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time >= %s AND create_time < %s ) tt "
            "GROUP BY tt.create_id ) txv ON txv.create_id = u.user_id "
            "LEFT JOIN ( SELECT tttoday.create_id, tttoday.create_name, count(tttoday.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tttoday GROUP BY tttoday.create_id ) txtoday ON txtoday.create_id = u.user_id "
            "LEFT JOIN corp_management.department dept ON dept.dept_id = u.dept_id "
            # "LEFT JOIN infrastructure.city ct ON ct.city_id = u.city_id "
            "LEFT JOIN (select u.user_id,d1.dept_name dept_name,d2.dept_name dept_name2,d3.dept_name dept_name3 from corp_management.user u,corp_management.department d,corp_management.department d1,corp_management.department d2,corp_management.department d3 "
            "where u.dept_id=d.dept_id and d.subdept_id=d1.dept_id and d1.subdept_id=d2.dept_id and d2.subdept_id=d3.dept_id) subdept ON subdept.user_id=u.user_id "
            "UNION "
            "SELECT u.fullname, IFNULL(tx.cre, 0), IFNULL(txv.cre, 0), IFNULL(txtoday.cre, 0)-IFNULL(tx.cre, 0),'--','--', IFNULL(dept.dept_name,0),IFNULL(subdept.dept_name,0) "
            "FROM ( SELECT user_id, fullname,dept_id, city_id FROM corp_management. USER cu WHERE cu.dept_id IN (SELECT dept_id from corp_management.department where subdept_id=13)) u "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tt GROUP BY tt.create_id ) tx ON tx.create_id = u.user_id "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time >= %s AND create_time < %s ) tt "
            "GROUP BY tt.create_id ) txv ON txv.create_id = u.user_id "
            "LEFT JOIN ( SELECT tttoday.create_id, tttoday.create_name, count(tttoday.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tttoday GROUP BY tttoday.create_id ) txtoday ON txtoday.create_id = u.user_id "
            "LEFT JOIN corp_management.department dept ON dept.dept_id = u.dept_id "
            "LEFT JOIN (select u.user_id,d1.dept_name dept_name,d2.dept_name dept_name2,d3.dept_name dept_name3 from corp_management.user u,corp_management.department d,corp_management.department d1,corp_management.department d2,corp_management.department d3 "
            "where u.dept_id=d.dept_id and d.subdept_id=d1.dept_id and d1.subdept_id=d2.dept_id and d2.subdept_id=d3.dept_id) subdept ON subdept.user_id=u.user_id "
            "UNION "
            "SELECT u.fullname, IFNULL(tx.cre, 0), IFNULL(txv.cre, 0), IFNULL(txtoday.cre, 0)-IFNULL(tx.cre, 0),'--', IFNULL(dept.dept_name,0),IFNULL(subdept.dept_name,0),IFNULL(subdept.dept_name2,0) "
            "FROM ( SELECT user_id, fullname,dept_id, city_id FROM corp_management. USER cu WHERE cu.dept_id IN (SELECT dept_id from corp_management.department where subdept_id IN (SELECT dept_id from corp_management.department where subdept_id=13))) u "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tt GROUP BY tt.create_id ) tx ON tx.create_id = u.user_id "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time >= %s AND create_time < %s ) tt "
            "GROUP BY tt.create_id ) txv ON txv.create_id = u.user_id "
            "LEFT JOIN ( SELECT tttoday.create_id, tttoday.create_name, count(tttoday.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tttoday GROUP BY tttoday.create_id ) txtoday ON txtoday.create_id = u.user_id "
            "LEFT JOIN corp_management.department dept ON dept.dept_id = u.dept_id "
            "LEFT JOIN (select u.user_id,d1.dept_name dept_name,d2.dept_name dept_name2,d3.dept_name dept_name3 from corp_management.user u,corp_management.department d,corp_management.department d1,corp_management.department d2,corp_management.department d3 "
            "where u.dept_id=d.dept_id and d.subdept_id=d1.dept_id and d1.subdept_id=d2.dept_id and d2.subdept_id=d3.dept_id) subdept ON subdept.user_id=u.user_id "
            "UNION "
            "SELECT u.fullname, IFNULL(tx.cre, 0), IFNULL(txv.cre, 0), IFNULL(txtoday.cre, 0)-IFNULL(tx.cre, 0), IFNULL(dept.dept_name,0),IFNULL(subdept.dept_name,0),IFNULL(subdept.dept_name2,0),IFNULL(subdept.dept_name3,0) "
            "FROM ( SELECT user_id, fullname,dept_id, city_id FROM corp_management. USER cu WHERE cu.dept_id IN (SELECT dept_id from corp_management.department where subdept_id IN (SELECT dept_id from corp_management.department where subdept_id IN (SELECT dept_id from corp_management.department where subdept_id=13)))) u "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tt GROUP BY tt.create_id ) tx ON tx.create_id = u.user_id "
            "LEFT JOIN ( SELECT tt.create_id, tt.create_name, count(tt.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time >= %s AND create_time < %s ) tt "
            "GROUP BY tt.create_id ) txv ON txv.create_id = u.user_id "
            "LEFT JOIN ( SELECT tttoday.create_id, tttoday.create_name, count(tttoday.create_id) cre "
            "FROM ( SELECT DISTINCT create_id, create_name, phone_num "
            "FROM customer WHERE create_time < %s ) tttoday GROUP BY tttoday.create_id ) txtoday ON txtoday.create_id = u.user_id "
            "LEFT JOIN corp_management.department dept ON dept.dept_id = u.dept_id "
            "LEFT JOIN (select u.user_id,d1.dept_name dept_name,d2.dept_name dept_name2,d3.dept_name dept_name3 from corp_management.user u,corp_management.department d,corp_management.department d1,corp_management.department d2,corp_management.department d3 "
            "where u.dept_id=d.dept_id and d.subdept_id=d1.dept_id and d1.subdept_id=d2.dept_id and d2.subdept_id=d3.dept_id) subdept ON subdept.user_id=u.user_id "
            ,
            [
                st, st, et, et,
                st, st, et, et,
                st, st, et, et,
                st, st, et, et
             ]
        )
        print 'select %s' %c
        self.obconn.close_conn(cursor, conn)
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            i=0
            for result in cursor.fetchall():
                l = tuple(list(result)+[st])
                # print i,l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8]
                cursorrd.execute(
                    'insert into customer_statistics(creator,customer_count,costomer_add_count,costomer_add_distinct_count,group_name,area,city_name,dept,date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8]]
                )
                # i+=1
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
            'delete from customer_statistics where date=%s', [d]
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
        ReadTable().to_kehutongjibiao(st, et)

    def clearTable(self, st=yd):
        ReadTable().check_kehutongjibiao(st)


Go().set_time()

# for i in xrange(120):
#     Go().set_time(yd-i*24*3600)
