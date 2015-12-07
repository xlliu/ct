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


#
#
# 当日上传的大楼图片数对应的楼盘数


    # 时间姓名角色新增房源 新增房源对应楼盘数 新增有图房源对应的图片数  当日上传的房源图片数   新增有图房源对应楼盘数     关闭房源    新增有图房源数  当日上传的房源图片对应的楼盘数
    # 时间姓名角色新增房源 新增房源对应楼盘数 新增房源对应照片数        新增照片数            新增照片对应楼盘数       删除房源数
class ReadTable(object):
    def __init__(self):
        ob = OffeceBuilding()
        rd = ReportDB()
        self.obconn = ConnectionDB(ob.dbuser, ob.dbpassword, ob.dbhost, ob.dbname, ob.dbport, ob.dbcharset)
        self.rdconn = ConnectionDB(rd.dbuser, rd.dbpassword, rd.dbhost, rd.dbname, rd.dbport, rd.dbcharset)
    def to_fangyuanlianghua_toId(self, st, et):
        connOb = self.obconn.get_conn()
        cursorOb = connOb.cursor()
        c = cursorOb.execute(
            'select '
            't1.creatorName,IFNULL(t9.dept_name,""),IFNULL(t2.new_house,0),'
            'IFNULL(t3.new_housetofloor,0),IFNULL(t4.new_housetophoto,0),'
            'IFNULL(t6.new_photo,0),IFNULL(t7.new_phototofloor,0),'
            'IFNULL(t8.del_floorsource,0), IFNULL(t10.new_house_inphoto,0),'
            'IFNULL(t12.tatalphoto_floor,0)'
            'from '
            '(((((((((('
            'select creator,creatorName '
            'from houseinfo '
            'where created_at >= %s and created_at<%s group by creator) '
            'UNION (select creator,creatorname '
            'from houseinfo_imgs '
            'where createat >= %s and createat<%s group by creator) '
            'UNION (select create_id creator,creator creatorname '
            'from housefollow '
            'where create_at >= %s and create_at<%s and statuschange=0 group by create_id)) t1 '
            # 新增房源√
            'LEFT JOIN (select creator,count(id) new_house '
            'from houseinfo '
            'where created_at >= %s and created_at<%s and status=1 GROUP BY creator) t2 '
            'ON t1.creator=t2.creator) '
            # 新增有图房源数√
            'left join (select ch.creator,count(ch.id) new_house_inphoto '
            'from (select distinct h.creator,h.Id from houseinfo h, houseinfo_imgs hm '
            'where h.id=hm.HouseInfoId and h.status=1 and h.created_at >= %s AND h.created_at < %s '
            'and hm.CreateAt >= %s AND hm.CreateAt < %s) ch group by ch.creator) t10 '
            'on t1.creator=t10.creator) '
            # 新增房源对应楼盘数√
            'LEFT JOIN (select ce.creator,count(ce.OfficeBuildingId) new_housetofloor '
            'from (select DISTINCT creator,OfficeBuildingId '
            'from houseinfo where status=1 and created_at >= %s and created_at<%s) ce GROUP BY ce.creator) t3 '
            'ON t1.creator=t3.creator) '
            # 新增有图房源对应的图片数
            'LEFT JOIN (select h.creator,count(hm.id) new_housetophoto '
            'from houseinfo h, houseinfo_imgs hm '
            'where h.id=hm.HouseInfoId and h.created_at >= %s and h.created_at<%s '
            'and hm.createat>= %s and hm.createat<%s and h.status=1 GROUP BY h.creator) t4 '
            'ON t1.creator=t4.creator) '
            # 当日上传的房源图片对应的楼盘数
            'left join (select xl.creator,count(xl.OfficeBuildingId) tatalphoto_floor '
            'from (select distinct hm.creator,OfficeBuildingId from houseinfo_imgs hm,houseinfo h,officebuilding ob '
            'where hm.CreateAt >=%s and hm.CreateAt<%s and hm.HouseInfoId=h.Id and h.OfficeBuildingId=ob.Id and h.status=1) xl '
            'GROUP BY xl.Creator) t12 '
            'on t1.creator=t12.Creator) '
            # 当日上传的房源图片数
            'LEFT JOIN (select creator,count(id) new_photo '
            'from houseinfo_imgs where CreateAt >= %s and CreateAt<%s group by creator) t6 '
            'ON t1.creator=t6.creator) '
            # 新增有图房源对应楼盘数
            'LEFT JOIN (select ch.creator,count(ch.OfficeBuildingId) new_phototofloor '
            'from (select distinct h.creator,h.OfficeBuildingId from houseinfo h, houseinfo_imgs hm '
            'where h.id=hm.HouseInfoId and h.created_at >= %s AND h.created_at < %s '
            'and hm.CreateAt >= %s AND hm.CreateAt < %s and h.status=1) ch group by ch.creator) t7 '
            'ON  t1.creator=t7.creator)'
            # 关闭房源
            'LEFT JOIN (select modifier creator,count(id) del_floorsource from houseinfo '
            'where modify_at >= %s and modify_at<%s and Status In (31,32,33) GROUP BY modifier) t8 '
            'ON t1.creator=t8.creator) '
            # 角色
            'LEFT JOIN	(select t.user_id,d.dept_name dept_name '
            'from (select user_id,dept_id '
            'from corp_management.user '
            'where user_id in (select creator as user_id '
            'from houseinfo '
            'where created_at >= %s and created_at<%s group by creator '
            'UNION select creator as user_id '
            'from houseinfo_imgs '
            'where createat >= %s and createat<%s group by creator '
            'union select create_id as creator '
            'from housefollow '
            'where create_at >= %s and create_at<%s and statuschange=0 '
            'group by create_id)) t,corp_management.department d '
            'where t.dept_id=d.dept_id) t9 ON t1.creator=t9.user_id'
            ,
            [
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et, st, et,
                st, et
            ]
        )
        print 'select %s' % c
        self.obconn.close_conn(cursorOb, connOb)
        # for result in cursorOb.fetchall():
        #     print result
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        try:
            for result in cursorOb.fetchall():
                l = tuple(list(result)+[st])
                # print l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10]
                cursorrd.execute(
                    'insert into new_house_work(creator_name,dept,add_house,add_house_floor,add_house_photo,'
                    'add_photo,add_photo_floor,del_house,new_house_inphoto,tatalphoto_floor,date) '
                    'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                    , [l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10]]
                )
            connrd.commit()
        except Exception, e:
            print 'add false', e
            connrd.rollback()
        finally:
            self.rdconn.close_conn(cursorrd, connrd)

    def check_Fangyuanlianghua_toId(self, d):
        connrd = self.rdconn.get_conn()
        cursorrd = connrd.cursor()
        d = cursorrd.execute(
            'delete from new_house_work where date=%s', [d]
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
        ReadTable().to_fangyuanlianghua_toId(st, et)

    def clearTable(self, st=yd):
        ReadTable().check_Fangyuanlianghua_toId(st)


Go().set_time()

# for i in xrange(90):
#   Go().set_time(yd-i*24*3600)
