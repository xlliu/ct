#!flask/bin/python
# -*- coding: utf-8 -*-
import sys
import time
import xlsxwriter

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import pymysql
import xlrd


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
        data = xlrd.open_workbook('test1.xlsx')
        table = data.sheet_by_index(0)
        workbook = xlsxwriter.Workbook('c://xlliu_update.xlsx')
        worksheet = workbook.add_worksheet()

        nrows = table.nrows
        ncols = table.ncols
        cursor = conn.cursor()
        for i in xrange(nrows):
            # 2015/9/1
            # t = time.strptime(table.row_values(i)[13], "%Y/%m/%d")
            # tim = int(time.mktime(t))
            print 'num start: %s' % i
            cursor.execute(
                "select followlook_date from followlook "
                "where follow_man=%s and customer_phone=%s and order_status=2 and followlook_date>=1443628800 and followlook_date<1446220800",
                [table.row_values(i)[5], table.row_values(i)[2]
                    # ,
                 # time.mktime(time.strptime(table.row_values(i)[0], '%Y-%m-%d')),
                 # time.mktime(time.strptime(table.row_values(i)[0], '%Y-%m-%d'))+24*3600
                 ]
            )
            followlook_okdate = cursor.fetchall()
            d = ""
            dn = []
            if followlook_okdate:
                for fo in followlook_okdate:
                    d = [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(f)) for f in fo]
                    dn.append(d)

            print "BusinessCircleId: %s" % dn if dn else "BusinessCircleId: null"

            for n in xrange(ncols):
                worksheet.write(i, n, table.row_values(i)[n])
                if n == ncols-1:
                    worksheet.write(i, int(n)+1, str(dn))
        workbook.close()
                # cursor.execute(
                #     "select district_id from infrastructure.district where short_name=%s",
                #     [table.row_values(i)[4]]
                # )
                # district_id = cursor.fetchall()
                # print "district_id: %s" % district_id[0][0] if district_id else "district_id: null"
                # cc = cursor.execute(
                #     "select * from officebuilding where CNName=%s and CityId=%s",
                #     [table.row_values(i)[3], 13]
                # )
                # a = [str(a[5]).decode(encoding='utf-8') for a in cursor.fetchall()]
                # print 'select num %s' % len(a)
                # ch = cursor.execute(
                #     "delete from officebuilding where CNName=%s and CityId=%s",
                #     [table.row_values(i)[0], 13]
                # )
                # conn.commit()
                # print 'delete %s' %ch
                # 'delete from new_sales_look_details where date=%s', [d]

        #         n = cursor.execute(
        #             "UPDATE officebuilding SET "
        #             "Address=%s,"
        #             "Districtid=%s,"
        #             "DistrictName=%s,"
        #             "BusinessCircleId=%s,"
        #             "BusinessCircleName=%s,"
        #
        #             "PropertyManagementLevel=%s,"
        #             "PropertyManagementer=%s,"
        #             "TotalCFA=%s,"
        #             "ConstructionRatio=%s,"
        #             "GroundFloor=%s,"
        #
        #             "PropertyManagementMoney=%s,"
        #             "PriceM2=%s,"
        #             "Description=%s,"
        #             "artremark=%s,"
        #             "FloorHeight=%s "
        #
        #             "where id=%s",
        #             [
        #                 table.row_values(i)[2],
        #                 district_id[0][0] if district_id else 0,
        #                 table.row_values(i)[4],
        #                 BusinessCircleId[0][0] if BusinessCircleId else 0,
        #                 table.row_values(i)[5],
        #
        #                 table.row_values(i)[6],
        #                 table.row_values(i)[7],
        #                 table.row_values(i)[8],
        #                 table.row_values(i)[10],
        #                 table.row_values(i)[9],
        #
        #                 table.row_values(i)[12],
        #                 table.row_values(i)[13],
        #                 table.row_values(i)[14],
        #                 table.row_values(i)[15],
        #                 table.row_values(i)[11],
        #
        #                 table.row_values(i)[0]
        #             ]
        #         )
        #         conn.commit()
        #         # print table.row_values(i)[2],district_id[0][0] if district_id else 0,table.row_values(i)[4],BusinessCircleId[0][0] if BusinessCircleId else 0,table.row_values(i)[5],\
        #         #     table.row_values(i)[6],table.row_values(i)[7],table.row_values(i)[8],table.row_values(i)[10],\
        #         #     table.row_values(i)[9],table.row_values(i)[12],table.row_values(i)[13],table.row_values(i)[14],\
        #         #     table.row_values(i)[15],table.row_values(i)[11],table.row_values(i)[0]
        #         print 'update data %s' %n
        #         print 'num end: %s' %i
        # except Exception, e:
        #     print 'errer %s' % e
        # finally:
        #     self.obconn.close_conn(cursor, conn)


class Go(object):
    def set_time(self):
        ReadTable().to_kehutongjibiao()


Go().set_time()