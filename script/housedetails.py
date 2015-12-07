#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import datetime, time
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
    # 房源名（大楼名+房源楼层号+房源房间号）房源照片数 录入时间 录入人 状态 部门 脚本执行时间
    def to_housedetails(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            "select concat(ob.CNName,'-',FloorNo,'-',HouseNo) housename,"
            "h.ImgCount houseimgs,h.created_at creat_time,h.creatorName creatorname,"
            "case h.status when 1 then '正常' when 33 then '已删除' when 32 then '已删除' when 31 then '已删除' when 40 then '信息不全' else '无法识别' end,"
            "concat(ob.DistrictName,'-',ob.BusinessCircleName) cir,"
            "dept.dept_name "
            "from houseinfo h,officebuilding ob,corp_management.user u,corp_management.department dept "
            "where h.OfficeBuildingId=ob.id and ob.cityid=12 "
            "and h.creator=u.user_id and u.dept_id=dept.dept_id and h.created_at<%s",
            [st]
        )
        print 'select %s' %c
        self.obconn.close_conn(cursor, conn)
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            cc = cursor.fetchall()
            for result in cc:
                l = list(result)+[st]
                l[0] = l[0].encode('GBK', 'ignore').decode('GBK')
                # print isinstance(ll,unicode), ll, l[1], l[2], l[3], l[4], l[5], l[6], l[7]
                cursorrd.execute(
                    'insert into house_details_statistics(housename,houseimgs,creat_time,creatorname,status,cir,dept,date) '
                    'values(%s,%s,%s,%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7]]
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
            'delete from house_details_statistics where date=%s', [d]
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

Go().set_time()

# for i in xrange(60):
#     Go().set_time(yd-i*24*3600)
