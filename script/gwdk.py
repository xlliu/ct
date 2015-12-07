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
    # dbhost = 'tianjixian.mysql.rds.aliyuncs.com'
    # dbuser = 'zhangjinglei'
    # dbpassword = 'zjl12315'
    # dbhost = 'ubanonline.mysql.rds.aliyuncs.com'
    # dbuser = 'online'
    # dbpassword = 'online12315'
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

    def to_GuWenDaiKan(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            'SELECT t.create_name, IFNULL(t2.cou_result, 0) asd, IFNULL(t3.cou_result, 0), '
            'IFNULL(t4.cou_result, 0), IFNULL(t5.cou_result, 0) '
            'FROM (((((SELECT user_id create_id,fullname create_name FROM corp_management.user u '
            'WHERE u.duty_status=1 and u.dept_id IN (SELECT d.dept_id FROM corp_management.department d '
            'WHERE FIND_IN_SET(d.dept_id, corp_management.queryChildrenDeptInfo(13)))) t '
            'left join (SELECT create_id, create_name, create_time '
            'FROM followlook WHERE create_time >= %s and create_time <%s '
            'GROUP BY create_id ) t1 '
            'on t.create_id=t1.create_id)'
            'LEFT JOIN ( SELECT create_id, count(create_id) cou_result '
            'FROM followlook WHERE order_status IN (1, 2) and create_time >= %s and create_time <%s '
            'GROUP BY create_id ) t2 '
            'ON t.create_id = t2.create_id ) '
            'LEFT JOIN ( SELECT create_id, count(create_id) cou_result '
            'FROM followlook WHERE order_status IN (2) and followlook_date >= %s and followlook_date<%s GROUP BY create_id ) t3 '
            'ON t.create_id = t3.create_id ) '
            'LEFT JOIN ( SELECT create_id, count(create_id) cou_result '
            'FROM followlook WHERE order_status IN (1, 2) GROUP BY create_id ) t4 ON t.create_id = t4.create_id ) '
            'LEFT JOIN ( SELECT create_id, count(create_id) cou_result '
            'FROM followlook WHERE order_status IN (2) GROUP BY create_id ) t5 ON t.create_id = t5.create_id'
            ,
            [
                st, et,
                st, et,
                st, et
            ]
        )
        print 'select %s' %c
        self.obconn.close_conn(cursor, conn)
        # for a in cursor.fetchall():
        #     for b in a:
        #         print b
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            for result in cursor.fetchall():
                l = tuple(list(result)+[st])
                cursorrd.execute(
                    'insert into new_sales_look_details(follow_man,add_look,finish_look,history_add_look,histort_finish_look,date) values(%s,%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5]]
                )
            connrd.commit()
        except Exception, e:
            print 'add false', e
            connrd.rollback()
        finally:
            self.rdconn.close_conn(cursorrd, connrd)

    def check_GuWenDaiKan(self, d):
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        d = cursorrd.execute(
            'delete from new_sales_look_details where date=%s', [d]
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
        ReadTable().to_GuWenDaiKan(st, et)

    def clearTable(self, st=yd):
        ReadTable().check_GuWenDaiKan(st)


Go().set_time()
# for i in xrange(3):
#     Go().set_time(yd-i*24*3600)