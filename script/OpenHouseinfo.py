#coding:utf-8
import sys

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'hejingjian'
import pymysql
import time,sys,xlsxwriter,datetime

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

def DBclose(a,b):
    a.close()
    b.close()

def CheckArgv():
    if len(sys.argv)>1:
        if len(sys.argv) == 2:
	    print u'to 2'
            exp_begin_time = int(time.mktime(time.strptime(str(sys.argv[1]),'%Y%m%d')))
            exp_end_time = int(time.mktime(time.strptime(str(sys.argv[1]),'%Y%m%d')))
            work_date = exp_end_time-86400
            print u"导出日期"+str(sys.argv[1])+"--"+str(sys.argv[1])+u"数据"
        elif len(sys.argv) == 3:
	    print u'to 3'
            exp_begin_time = int(time.mktime(time.strptime(sys.argv[1],'%Y%m%d')))
            exp_end_time = int(time.mktime(time.strptime(str(sys.argv[2]),'%Y%m%d')))
            work_date = exp_end_time
            print u"导出日期"+str(sys.argv[1])+"--"+str(sys.argv[2])+u"数据"
        else:
            print u"传入的参数不符，请输入日期，样例：python py文件 20150101 或 python py文件 20150101 20150105"
            sys.exit()
    else:
	print u'to no'
        exp_begin_time = getToday(-1)
        exp_end_time = getToday()
        nowdate=time.localtime(time.time())
        nowdate=time.strftime("%Y%m%d", nowdate)
        work_date = exp_begin_time
        print u"导出日期"+str(nowdate)+"--"+str(nowdate)+u"数据"
    return exp_begin_time,exp_end_time,work_date

def WtExcel(sheet,msg):
    row_y = 1
    for item in msg:
        row_x = 0
        for i in item:
            if row_x ==1:
                ltime=time.localtime(i)
                i=time.strftime("%Y-%m-%d", ltime)
            sheet.write(row_y, row_x, i)
            row_x+=1
        row_y+=1

def SelDB(cur,sql,begin_time,end_time):
    cur.execute(sql,(begin_time,end_time))
    msg = cur.fetchall()
    return msg

def WtColum(columlist,sheet):
    i = 0
    for colum in columlist:
        sheet.write(0,i,colum)
        i+=1

def expexcel():
    wb = xlsxwriter.Workbook('/data/exporttmp/Analyse_houseinfo.xlsx');
    analyse_houseinfo = wb.add_worksheet('analyse_houseinfo');
    analyse_houseinfo_colum = [u'id',u'日期',u'有房源大楼数',u'已发布房源数',u'没有房源大楼数',u'未发布房源']
    WtColum(analyse_houseinfo_colum,analyse_houseinfo)
    DBhouseinfo = pymysql.connect(host='rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com',port=3306,user='online',passwd='online12315',db='reportdb',charset='utf8')
    houseinfo = DBhouseinfo.cursor()

    sql = "select id,work_date,house_build_num,house_num,nothouse_build_num,nothouse_num from analyse_houseinfo where work_date >=%s AND work_date <%s ORDER BY work_date"
    msg = SelDB(houseinfo,sql,exp_begin_time,exp_end_time)
    DBhouseinfo.close()
    houseinfo.close()
    WtExcel(analyse_houseinfo,msg)
    wb.close()



exp_begin_time,exp_end_time,work_date=CheckArgv()
begin_time = getToday(-1)
end_time =getToday()

#corp_management = pymysql.connect(host='tianjixian.mysql.rds.aliyuncs.com',port=3306,user='zhangjinglei',passwd='zjl12315',db='corp_management',charset='utf8')
corp_management = pymysql.connect(host='rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com',port=3306,user='online',passwd='online12315',db='corp_management',charset='utf8')
cur = corp_management.cursor()
sql = """
SELECT
(SELECT COUNT(*) FROM corp_officebuilding.`officebuilding` WHERE STATUS = 1 AND cityid = 12 AND id IN (
SELECT DISTINCT officebuildingid FROM corp_officebuilding.houseinfo WHERE STATUS = 1) and 
officebuilding.publishdate<%s) AS  house_build_num,
(SELECT COUNT(*) FROM corp_officebuilding.houseinfo WHERE STATUS=1) AS house_num,
(SELECT COUNT(*) FROM corp_officebuilding.`officebuilding` WHERE STATUS = 1 AND cityid = 12 AND id NOT IN (
SELECT DISTINCT officebuildingid FROM corp_officebuilding.houseinfo WHERE STATUS = 1) and 
officebuilding.publishdate<%s) AS  nothouse_build_num,
(SELECT COUNT(*) FROM corp_officebuilding.houseinfo WHERE STATUS NOT IN (1,31,32,33)) AS nothouse_num FROM
corp_officebuilding.officebuilding where officebuilding.publishdate<%s LIMIT 1
"""
cur.execute(sql,[exp_end_time,exp_end_time,exp_end_time])
msg = cur.fetchall()

reportdb = pymysql.connect(host='rds75fa0kz8u0lcvqmmd1.mysql.rds.aliyuncs.com',port=3306,user='online',passwd='online12315',db='reportdb',charset='utf8')
# reportdb = pymysql.connect(host='tianjixian.mysql.rds.aliyuncs.com',port=3306,user='zhangjinglei',passwd='zjl12315',db='reportdb',charset='utf8')
curdata = reportdb.cursor()


delsql = "delete from analyse_houseinfo where work_date=%s"
curdata.execute(delsql,(work_date))

for item in msg:
   house_build_num = item[0]
   house_num = item[1]
   nothouse_build_num = item[2]
   nothouse_num = item[3]
   sql = "insert into analyse_houseinfo (work_date,house_build_num,house_num,nothouse_build_num,nothouse_num) VALUE (%s,%s,%s,%s,%s)"
   curdata.execute(sql,(work_date,house_build_num,house_num,nothouse_build_num,nothouse_num))
   reportdb.commit()

DBclose(curdata,reportdb)
DBclose(cur,corp_management)
expexcel()









