#!flask/bin/python
# -*- coding: utf-8 -*-
import sys
import time

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import pymysql
import xlrd

"""
插入楼盘，存在跳过
"""


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
        data = xlrd.open_workbook('charuloupan.xlsx')
        table = data.sheet_by_index(0)
        nrows = table.nrows
        cursor = conn.cursor()
        try:
            for i in xrange(1, nrows):
                print 'num start: %s' %i
                cursor.execute(
                    "select BusinessCircleId from infrastructure.businesscircle where BusinessCircleName=%s",
                    [table.row_values(i)[3]]
                )
                BusinessCircleId = cursor.fetchall()
                print "BusinessCircleId: %s" % BusinessCircleId[0][0] if BusinessCircleId else "BusinessCircleId: null"
                cursor.execute(
                    "select district_id from infrastructure.district where short_name=%s and city_id=%s",
                    [table.row_values(i)[2], 13]
                )
                district_id = cursor.fetchall()
                print "district_id: %s" % district_id[0][0] if district_id else "district_id: null"
                nn = cursor.execute(
                    "select * from officebuilding where CNName=%s and CityId=%s ",
                    [table.row_values(i)[0], 13]
                )
                for a in cursor.fetchall():
                    print 'select: have data' + str(a[0]).decode(encoding='utf-8')
                if str(nn)=="0":
                    n = cursor.execute(
                    "insert into officebuilding("
                    "DeveloperCompany,Districtid,"
                    "DistrictName,BusinessCircleId,"
                    "BusinessCircleName,PropertyManagementLevel,"

                    "PropertyManagementer,TotalCFA,"
                    "ConstructionRatio,GroundFloor,"
                    "UndergroundFloor,PropertyManagementMoney,"

                    "PriceM2,Description,"
                    "LandAreaSize,CNName,"
                    "CityId,CityName,"

                    "TimeOfCompletion,PropertyManagementType,"
                    "Creator,CreatorName,"
                    "CreateAt"
                    ") values ("
                    "%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s,%s,"
                    "%s,%s,%s,%s,%s)"
                    ,
                    [
                        table.row_values(i)[1], district_id[0][0] if district_id else 0,
                        table.row_values(i)[2], BusinessCircleId[0][0] if BusinessCircleId else 0,
                        table.row_values(i)[3], table.row_values(i)[4],

                        table.row_values(i)[5], table.row_values(i)[6],
                        table.row_values(i)[7], table.row_values(i)[8] if table.row_values(i)[8] else 0,
                        table.row_values(i)[9] if table.row_values(i)[9] else 0, table.row_values(i)[10],

                        table.row_values(i)[11], table.row_values(i)[12],
                        table.row_values(i)[13], table.row_values(i)[0],
                        13, "上海",

                        time.mktime(time.strptime(table.row_values(i)[14],"%Y.%m.%d")) if table.row_values(i)[14] else 0, table.row_values(i)[15],
                        19897, "刘雪龙", 1446566400
                    ]
                    )
                    print "insert:%s" % n
                else:
                    print u"查到了%s条，不写" %str(nn)
            conn.commit()
        except Exception, e:
            print 'errer %s' %e
            conn.rollback()
        finally:
            self.obconn.close_conn(cursor, conn)


class Go(object):
    def set_time(self):
        ReadTable().to_kehutongjibiao()


Go().set_time()