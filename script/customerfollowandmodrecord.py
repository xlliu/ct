#!flask/bin/python
# -*- coding: utf-8 -*-
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
            "SELECT tt1.dept, tt1.city, tt1.area, tt1.mingroup, tt.* FROM ( SELECT t.id, IFNULL(t.customer_name, '') 客户名, IFNULL(t.orderid, '') 订单id, IFNULL(t.source, '') 来源, IFNULL(t.create_time, '') 创建时间, t.follow_id, IFNULL(t.follow_man, '') 带看人, IFNULL(t.house_id, '') 房源id, IFNULL(t1.带看总次数, ''), IFNULL(t2.带看总房源数, ''), IFNULL( t3.第一次跟进时间, '' ), IFNULL( t4.`第一次带看时间`, '' ), IFNULL( t4.`第一次带看房源号`, '' ), IFNULL( t5.`最后一次跟进时间`, '' ), IFNULL(t6.`跟进总次数`, '') FROM ( SELECT c.id, c.customer_name, CASE c.orderid WHEN '' THEN '个人录入' ELSE '订单分配' END orderid, CASE c.source WHEN 0 THEN '全部' WHEN 1 THEN '个人用户' WHEN 2 THEN '400用户' WHEN 3 THEN '主站' WHEN 4 THEN '赶集' WHEN 5 THEN '58' WHEN 6 THEN '安居客' WHEN 7 THEN '搜房' WHEN 8 THEN '其他网站' WHEN 9 THEN '老用户' WHEN 10 THEN '公众号' WHEN 11 THEN '电话销售' END source, FROM_UNIXTIME(c.create_time) create_time, f.follow_id, f.follow_man, f.house_id FROM corp_officebuilding.customer c, ( SELECT DISTINCT * FROM corp_officebuilding.followlook ) f, corp_management. USER u WHERE c.id = f.customer_id AND f.follow_id = u.user_id AND f.order_status = 2 AND c.create_time >= % s AND c.create_time < % s AND f.followlook_date >= % s AND f.followlook_date < % s ) t LEFT JOIN ( SELECT c.id, f.follow_id, count(c.id) '带看总次数' FROM corp_officebuilding.customer c, ( SELECT DISTINCT * FROM corp_officebuilding.followlook ) f, corp_management. USER u WHERE c.id = f.customer_id AND f.follow_id = u.user_id AND f.order_status = 2 AND c.create_time >= % s AND c.create_time < % s AND f.followlook_date >= % s AND f.followlook_date < % s GROUP BY c.id, f.follow_id ) t1 ON t.follow_id = t1.follow_id AND t.id = t1.id LEFT JOIN ( SELECT c.id, m.create_id, count(c.id) '跟进总次数' FROM corp_officebuilding.customer c, ( SELECT c.id, m.create_id FROM corp_officebuilding.customer c, ( SELECT DISTINCT * FROM corp_officebuilding.modrecord ) m, corp_management. USER u WHERE c.id = m.customer_id AND m.create_id = u.user_id AND m.type IN (1, 2, 3, 4, 5, 6) AND c.create_time >= % s AND c.create_time < % s AND m.create_time >= % s AND m.create_time < % s ) m GROUP BY c.id, m.create_id ) t6 ON t.follow_id = t6.create_id AND t.id = t6.id LEFT JOIN ( SELECT t.id, t.follow_id, count(t.house_id) '带看总房源数' FROM ( SELECT c.id, f.house_id, f.follow_id FROM corp_officebuilding.customer c, ( SELECT DISTINCT * FROM corp_officebuilding.followlook ) f, corp_management. USER u WHERE c.id = f.customer_id AND f.follow_id = u.user_id AND f.order_status = 2 AND c.create_time >= % s AND c.create_time < % s AND f.followlook_date >= % s AND f.followlook_date < % s GROUP BY c.id, f.house_id, f.follow_id ) t GROUP BY t.id, t.follow_id ) t2 ON t.follow_id = t2.follow_id AND t.id = t2.id LEFT JOIN ( SELECT t.id, t.create_id, t.create_time '第一次跟进时间' FROM ( SELECT c.id, m.create_id, FROM_UNIXTIME(m.create_time) create_time FROM corp_officebuilding.customer c, ( SELECT DISTINCT * FROM corp_officebuilding.modrecord ) m, corp_management. USER u WHERE c.id = m.customer_id AND m.create_id = u.user_id AND m.type IN (1, 2, 3, 4, 5, 6) AND c.create_time >= % s AND c.create_time < % s AND m.create_time >= % s AND m.create_time < % s ORDER BY m.create_time ASC ) t GROUP BY t.id, t.create_id ) t3 ON t.id = t3.id AND t.follow_id = t3.create_id LEFT JOIN ( SELECT t.id, t.follow_id, t.house_id '第一次带看房源号', t.followlook_date '第一次带看时间' FROM ( SELECT c.id, f.follow_id, f.house_id, FROM_UNIXTIME(f.followlook_date) followlook_date FROM corp_officebuilding.customer c, ( SELECT DISTINCT * FROM corp_officebuilding.followlook ) f, corp_management. USER u WHERE c.id = f.customer_id AND f.follow_id = u.user_id AND f.order_status = 2 AND c.create_time >= % s AND c.create_time < % s AND f.followlook_date >= % s AND f.followlook_date < % s ORDER BY f.followlook_date ASC ) t GROUP BY t.id, t.follow_id ) t4 ON t.id = t4.id AND t.follow_id = t4.follow_id LEFT JOIN ( SELECT t.id, t.create_id, t.create_time '最后一次跟进时间' FROM ( SELECT c.id, m.create_id, FROM_UNIXTIME(m.create_time) create_time FROM corp_officebuilding.customer c, ( SELECT DISTINCT * FROM corp_officebuilding.modrecord ) m, corp_management. USER u WHERE c.id = m.customer_id AND m.create_id = u.user_id AND m.type IN (1, 2, 3, 4, 5, 6) AND c.create_time >= % s AND c.create_time < % s AND m.create_time >= % s AND m.create_time < % s ORDER BY m.create_time DESC ) t GROUP BY t.id, t.create_id ) t5 ON t.id = t5.id AND t.follow_id = t5.create_id ) tt, ( SELECT IFNULL(dept.dept_name, \"\") AS \"dept\", \"-\" AS \"city\", \"-\" AS \"area\", \"-\" AS \"mingroup\", ui.fullname AS \"customer\", ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), \"-\", ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" )))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id UNION SELECT IFNULL(subdept.dept_name3, \"\"), IFNULL(subdept.dept_name2, \"\"), IFNULL(subdept.dept_name, \"\"), IFNULL(dept.dept_name, \"\"), ui.fullname, ui.user_id FROM ( SELECT u.fullname, u.user_id, u.dept_id FROM corp_management. USER u WHERE u.dept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department d WHERE d.subdept_id IN ( SELECT dept_id FROM corp_management.department WHERE deptcode = \"/gwb\" ))))) ui LEFT JOIN corp_management.department dept ON dept.dept_id = ui.dept_id LEFT JOIN ( SELECT u.user_id, d1.dept_name dept_name, d2.dept_name dept_name2, d3.dept_name dept_name3 FROM corp_management. USER u, corp_management.department d, corp_management.department d1, corp_management.department d2, corp_management.department d3 WHERE u.dept_id = d.dept_id AND d.subdept_id = d1.dept_id AND d1.subdept_id = d2.dept_id AND d2.subdept_id = d3.dept_id ) subdept ON subdept.user_id = ui.user_id ) tt1 WHERE tt.follow_id = tt1.user_id"
            ,
            [
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et
            ]
        )
        print 'select %s' % c
        self.obconn.close_conn(cursor, conn)
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            for result in cursor.fetchall():
                l = tuple(list(result) + [st])
                # print l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14], l[15], l[16], l[17]
                cursorrd.execute(
                    'insert into customerfollowandmodrecord('
                    'dept,city,area,mingroup,customerid,'
                    'customername,orderid,source,create_time,followid,'
                    'follow_man,house_id,followcount,followhousecount,firstmodrecordtime,'
                    'firstfollowlooktime,firstfollowhouseid,lastmodrecordtime,modrecordcount,data) values('
                    '%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s,'
                    '%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14], l[15], l[16], l[17], l[18], l[19]]
                )
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
            'delete from customerfollowandmodrecord where data=%s', [d]
        )
        print 'delete %s' % d
        connrd.commit()
        self.rdconn.close_conn(cursorrd, connrd)


yd = getToday(-1)


class Go(object):
    def set_time(self, st=yd):
        print st
        et = st + 24 * 3600
        self.clearTable(st)
        ReadTable().to_kehutongjibiao(st, et)

    def clearTable(self, st=yd):
        ReadTable().check_kehutongjibiao(st)


Go().set_time()

# for i in xrange(90):
#    Go().set_time(yd - i * 24 * 3600)
