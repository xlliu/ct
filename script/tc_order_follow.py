#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import datetime, time
import pymysql
import xlsxwriter


class OrderCenter(object):
    dbhost = 'rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com'
    dbuser = 'online'
    dbpassword = 'online12315'
    #dbhost = 'ubanonline.mysql.rds.aliyuncs.com'
    #dbuser = 'online'
    #dbpassword = 'online12315'
    # dbhost = 'tianjixian.mysql.rds.aliyuncs.com'
    # dbuser = 'zhangjinglei'
    # dbpassword = 'zjl12315'
    dbport = 3306
    dbname = 'ordercenter'
    dbcharset = 'utf8'


class ReportDB(object):
    dbhost = 'rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com'
    dbuser = 'online'
    dbpassword = 'online12315'
    #dbhost = 'tianjixian.mysql.rds.aliyuncs.com'
    #dbuser = 'zhangjinglei'
    #dbpassword = 'zjl12315'
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
        oc = OrderCenter()
        rd = ReportDB()
        self.obconn = ConnectionDB(oc.dbuser, oc.dbpassword, oc.dbhost, oc.dbname, oc.dbport, oc.dbcharset)
        self.rdconn = ConnectionDB(rd.dbuser, rd.dbpassword, rd.dbhost, rd.dbname, rd.dbport, rd.dbcharset)


        # 订单编号tc_order.orderid  发起时间（tc_order.create_at）
        # 分配时间（workitem_log.create_at）   分配对象（workitem_log.tousername）
        #  跟进人tracklog.username  跟进时间tracklog.create_at  跟进内容tracklog.remark
        #（如果有多条跟进记录，则给多条数据）

    def tc_order_follow(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        n = cursor.execute(
            "select tc.orderid,IFNULL(tc.create_at,0),IFNULL(wl.create_at,0),IFNULL(wl.tousername,''),"
            "IFNULL(tl.username,''),IFNULL(tl.create_at,0),IFNULL(tl.remark,'') "
            "from tc_order tc "
            "LEFT JOIN workitem_log wl on tc.orderid=wl.orderid "
            "LEFT JOIN tracklog tl on tc.orderid=tl.orderid "
            "where tc.create_at>=%s and tc.create_at<%s and tc.`status`=30",
            [st, et]
        )
        print 'select: n:%s' % n
        conn.close()
        return cursor.fetchall()

    def add_result(self, st, et):
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            results = self.tc_order_follow(st, et)
            for result in results:

                l = list(result)+[st]
                # l[0] = l[0].encode('GBK', 'ignore').decode('GBK')
                # print l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7]
                cursorrd.execute(
                    'insert into tc_order_follow(orderid,time_of_order,allocate_time,allocate_name,follow_man,follow_time,follow_content,date) '
                    'values(%s,%s,%s,%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7]]
                )
            connrd.commit()
        except Exception, e:
            print 'err', e
            connrd.rollback()
        finally:
            self.rdconn.close_conn(cursorrd, connrd)

    def check_housedetails(self, d):
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        c = cursorrd.execute(
            'delete from tc_order_follow where date=%s', [d]
        )
        print 'delete: %s' %c
        connrd.commit()
        self.rdconn.close_conn(cursorrd, connrd)

    def write_excel(self):

        workbook = xlsxwriter.Workbook('yifenpeidingdan.xlsx')
        worksheet = workbook.add_worksheet()
        bold = workbook.add_format({'bold': 1})

        # 这是个数据table的列
        headings = ['订单编号', '发起时间', '分配时间', '分配对象','跟进人', '跟进时间', '跟进内容']
        row = 1
        hd = self.tc_order_follow()
        for r in hd:
            # print r[0], str(r[1]), str(r[2]), r[3], r[4], str(r[5]), r[6]
            a = []
            for resu in r:
                a.append(resu)

            # print type(a[1])
            if a[1]:
                a[1] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(a[1])))
            if a[2]:
                a[2] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(a[2])))
            if a[5]:
                a[5] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(a[5])))

            worksheet.write_row(0, 0, headings, bold)
            worksheet.write_row(row, 0, a)
            row += 1
        workbook.close()


yd = getToday(-1)


class Go(object):

    def set_time(self, st=yd):
        print st
        et = st +24*3600
        self.clearTable(st)
        ReadTable().add_result(st, et)

    def clearTable(self, st=yd):
        ReadTable().check_housedetails(st)

Go().set_time()

#for i in xrange(6):
#    Go().set_time(yd-i*24*3600)



