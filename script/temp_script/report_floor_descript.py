#!flask/bin/python
# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import pymysql
import xlrd


class OffeceBuilding(object):
    # dbhost = 'rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com'
    # dbuser = 'online'
    # dbpassword = 'online12315'
    # dbhost = 'ubanonline.mysql.rds.aliyuncs.com'
    # dbuser = 'online'
    # dbpassword = 'online12315'
    dbhost = 'tianjixian.mysql.rds.aliyuncs.com'
    dbuser = 'zhangjinglei'
    dbpassword = 'zjl12315'
    dbport = 3306
    dbname = 'corp_officebuilding'
    dbcharset = 'utf8'


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

    def to_kehutongjibiao(self):
        conn = self.obconn.get_conn()
        data = xlrd.open_workbook('import_floor_descript.xlsx')
        table = data.sheet_by_index(0)
        nrows = table.nrows
        cursor = None
        try:
            for i in xrange(1, nrows):
                print 'num start: %s' %i
                cursor = conn.cursor()
                cursor.execute(
                    "select * from officebuilding where CNName=%s and CityId=%s ",
                    [table.row_values(i)[0], 12]
                )
                for a in cursor.fetchall():
                    print 'select: have data' + str(a[5]).encode('gbk', 'ignore').decode(encoding='gbk')
                if not table.row_values(i)[1] and table.row_values(i)[2]:
                    cursor.execute(
                    "UPDATE officebuilding SET "
                    "artremark=%s "
                    "where CNName=%s and CityId=%s",
                    [
                         table.row_values(i)[2],
                         table.row_values(i)[0], 12
                    ]
                    )
                else:
                    cursor.execute(
                    "UPDATE officebuilding SET "
                    "Description=%s,"
                    "artremark=%s "
                    "where CNName=%s and CityId=%s",
                     [
                         table.row_values(i)[1],
                         table.row_values(i)[2],
                         table.row_values(i)[0], 12
                      ]
                    )
                print 'num end: %s' %i
        except Exception, e:
            print 'errer %s' %e
        finally:
            conn.commit()
            self.obconn.close_conn(cursor, conn)


class Go(object):
    def set_time(self):
        ReadTable().to_kehutongjibiao()


Go().set_time()