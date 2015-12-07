#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import datetime
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
    dbname = 'ordercenter'
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
    # 时间  姓名  身份 400  主站  电销
    def to_housedetails(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            "select distinct tc.creator,tc.fullname,dept.dept_name,IFNULL(t1.s1,0),IFNULL(t2.s2,0),IFNULL(t3.s3,0) from "
            "(select u.user_id creator,u.fullname from corp_management.user u where u.dept_id in (14,27) and u.duty_status=1) tc "
            "left join "
            "(select u.user_id creator,d.dept_name from corp_management.user u,corp_management.department d where u.dept_id=d.dept_id and u.duty_status=1) dept on tc.creator=dept.creator "
            "left join "
            "(select tc.creator,tc.creator_name,tc.source,COUNT(tc.source) s1 from tc_order tc where tc.source=1 and tc.status not in (10, 20, 70) and tc.create_at>=%s and tc.create_at<%s group by tc.creator,tc.source) t1 on tc.creator=t1.creator "
            "left join "
            "(select tc.creator,tc.creator_name,tc.source,COUNT(tc.source) s2 from tc_order tc where tc.source=2 and tc.status not in (10, 20, 70) and tc.create_at>=%s and tc.create_at<%s group by tc.creator,tc.source) t2 on tc.creator=t2.creator "
            "left join "
            "(select tc.creator,tc.creator_name,tc.source,COUNT(tc.source) s3 from tc_order tc where tc.source=3 and tc.status not in (10, 20, 70) and tc.create_at>=%s and tc.create_at<%s group by tc.creator,tc.source) t3 on tc.creator=t3.creator ",
            [
                st, et, st, et,
                st, et
             ]
        )
        print 'select %s' %c
        self.obconn.close_conn(cursor, conn)
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            cc = cursor.fetchall()
            for result in cc:
                l = list(result)+[st]
                # l[0] = l[0].encode('GBK', 'ignore').decode('GBK')
                print l[1], l[2], l[3], l[4], l[5], l[6]
                cursorrd.execute(
                    'insert into customer_server_work(name,dept_name,400_phone,master_station,telemarketing,date) '
                    'values(%s,%s,%s,%s,%s,%s)'
                    , [l[1], l[2], l[3], l[4], l[5], l[6]]
                )
            connrd.commit()
        except Exception, e:
            print 'add false', e
            connrd.rollback()
        finally:
            self.rdconn.close_conn(cursorrd, connrd)

    def check_housedetails(self, d):
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        d = cursorrd.execute(
            'delete from customer_server_work where date=%s', [d]
        )
        print 'delete %s' %d
        connrd.commit()
        self.rdconn.close_conn(cursorrd, connrd)


yd = getToday(-1)


class Go(object):

    def set_time(self, st=yd):
        print st
        et = st +24*3600
        self.clearTable(st)
        ReadTable().to_housedetails(st, et)

    def clearTable(self, st=yd):
        ReadTable().check_housedetails(st)

# Go().set_time()

for i in xrange(60):
    Go().set_time(yd-i*24*3600)
