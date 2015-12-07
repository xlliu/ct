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
    dbname = 'corp_management'
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
    # 城市   大区  小组   顾问  新增房源数   设为待租房源数   设为已租房源数   设为无效房源数  日期
    def to_kehutongjibiao(self, st, et):
        conn = self.obconn.get_conn()
        cursor = conn.cursor()
        c = cursor.execute(
            "SELECT ( CASE WHEN USER .`status` = 2 THEN '在职' WHEN USER .`status` = 3 THEN '离职' END ) AS '状态',"
            " entry_date AS '入职日期', USER .fullname AS '姓名',"
            " USER .`user_id`, USER .`dept_id`, "
            "( SELECT dept_name FROM corp_management.department WHERE dept_id = USER .`dept_id` ) AS '部门',"
            " ( SELECT COUNT(*) FROM `corp_officebuilding`.Houseinfo "
            "WHERE created_at >=% s AND created_at <% s AND STATUS = 1 AND creator = USER .`user_id` ) AS '新增房源', "
            "( SELECT COUNT(*) FROM `corp_officebuilding`.`customer` "
            "WHERE create_time >=% s AND create_time <% s AND source != 2 AND source != 3 AND create_id = USER .`user_id` ) AS '新增客户', "
            "( SELECT COUNT(*) FROM "
            "( SELECT DISTINCT ( CONCAT(customer_phone, create_id)) AS num, customer_name, create_id, create_name "
            "FROM `corp_officebuilding`.`followlook` WHERE followlook_date >=% s AND followlook_date <% s ) AS a "
            "WHERE a.create_id = USER .`user_id` ) AS '新增带看' FROM USER WHERE USER .`status` = 2 AND user_id IN "
            "( SELECT user_id FROM corp_management. USER cu WHERE cu.dept_id IN ( SELECT dept_id FROM corp_management.department "
            "WHERE subdept_id IN ( SELECT dept_id FROM corp_managem  ent.department "
            "WHERE subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE subdept_id = 13 )))) "
            "UNION "
            "SELECT ( CASE WHEN USER .`status` = 2 THEN '在职' WHEN USER .`status` = 3 THEN '离职' END ) AS '状态', "
            "entry_date AS '入职日期', USER .fullname AS '姓名', USER .`user_id`, USER .`dept_id`, "
            "( SELECT dept_name FROM corp_management.department WHERE dept_id = USER .`dept_id` ) AS '部门', "
            "( SELECT COUNT(*) FROM `corp_officebuilding`.Houseinfo "
            "WHERE created_at >=% s AND created_at <% s AND STATUS = 1 AND creator = USER .`user_id` ) AS '新增房源', "
            "( SELECT COUNT(*) FROM `corp_officebuilding`.`customer` "
            "WHERE create_time >=% s AND create_time <% s AND source != 2 AND source != 3 AND create_id = USER .`user_id` ) AS '新增客户', "
            "( SELECT COUNT(*) FROM ( SELECT DISTINCT ( CONCAT(customer_phone, create_id)) AS num, customer_name, create_id, create_name "
            "FROM `corp_officebuilding`.`followlook` WHERE followlook_date >=% s AND followlook_date <% s ) AS a "
            "WHERE a.create_id = USER .`user_id` ) AS '新增带看' FROM USER WHERE USER .`status` = 2 AND user_id IN "
            "( SELECT user_id FROM corp_management. USER cu WHERE cu.dept_id IN ( SELECT dept_id FROM corp_management.department "
            "WHERE subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE subdept_id = 13 ))) "
            "UNION "
            "SELECT ( CASE WHEN USER .`status` = 2 THEN '在职' WHEN USER .`status` = 3 THEN '离职' END ) AS '状态', "
            "entry_date AS '入职日期', USER .fullname AS '姓名', USER .`user_id`, USER .`dept_id`, "
            "( SELECT dept_name FROM corp_management.department WHERE dept_id = USER .`dept_id` ) AS '部门', "
            "( SELECT COUNT(*) FROM `corp_officebuilding`.Houseinfo "
            "WHERE created_at >=% s AND created_at <% s AND STATUS = 1 AND creator = USER .`user_id` ) AS '新增房源', "
            "( SELECT COUNT(*) FROM `corp_officebuilding`.`customer` "
            "WHERE create_time >=% s AND create_time <% s AND source != 2 AND source != 3 AND create_id = USER .`user_id` ) AS '新增客户', "
            "( SELECT COUNT(*) FROM ( SELECT DISTINCT "
            "( CONCAT(customer_phone, create_id)) AS num, customer_name, create_id, create_name FROM `corp_officebuilding`.`followlook` "
            "WHERE followlook_date >=% s AND followlook_date <% s ) AS a WHERE a.create_id = USER .`user_id` ) AS '新增带看' "
            "FROM USER WHERE USER .`status` = 2 AND user_id IN "
            "( SELECT user_id FROM corp_management. USER cu WHERE cu.dept_id IN ( SELECT dept_id FROM corp_management.department "
            "WHERE subdept_id = 13 )) "
            "UNION "
            "SELECT ( CASE WHEN USER .`status` = 2 THEN '在职' "
            "WHEN USER .`status` = 3 THEN '离职' END ) AS '状态', "
            "entry_date AS '入职日期', USER .fullname AS '姓名', USER .`user_id`, USER .`dept_id`, "
            "( SELECT dept_name FROM corp_management.department WHERE dept_id = USER .`dept_id` ) AS '部门', "
            "( SELECT COUNT(*) FROM `corp_officebuilding`.Houseinfo WHERE created_at >=% s AND created_at <% s "
            "AND STATUS = 1 AND creator = USER .`user_id` ) AS '新增房源', "
            "( SELECT COUNT(*) FROM `corp_officebuilding`.`customer` "
            "WHERE create_time >=% s AND create_time <% s AND source != 2 AND source != 3 AND create_id = USER .`user_id` ) AS '新增客户', "
            "( SELECT COUNT(*) FROM ( SELECT DISTINCT ( CONCAT(customer_phone, create_id)) AS num, customer_name, create_id, create_name "
            "FROM `corp_officebuilding`.`followlook` WHERE followlook_date >=% s AND followlook_date <% s ) AS a "
            "WHERE a.create_id = USER .`user_id` ) AS '新增带看' FROM USER WHERE USER .`status` = 2 AND user_id IN ( "
            "SELECT user_id FROM corp_management. USER cu WHERE cu.dept_id = 13 )"
            ,
            [
                st, et, st, et, st, et,
                st, et, st, et, st, et,
                st, et, st, et, st, et,
                st, et, st, et, st, et
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
                    'insert into customer_work_status(status,entry_date,name,name_id,dept_id,dept,new_add_house,new_add_costomer,new_add_follow_look,date,didi,deal,remarks) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], 0, 0, '']
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
            'delete from customer_work_status where date=%s', [d]
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

# for i in xrange(4):
#     Go().set_time(yd-i*24*3600)
