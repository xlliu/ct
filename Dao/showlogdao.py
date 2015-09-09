#!flask/bin/python
#-*- coding: utf-8 -*-
import time
from Dal.controltower import Log, Job

__author__ = 'xlliu'


class ShowLogDao(object):

    def showLog(self, id):
        if id:
            data = Log.select(Log, Job).join(Job, on=(Log.job == Job.id).alias('jobresult')).where(Job.id==id).order_by(Log.begin.desc()).limit(29)
        else:
            data = Log.select(Log, Job).join(Job, on=(Log.job == Job.id).alias('jobresult')).order_by(Log.begin.desc()).limit(29)
        for d in data:
            s = d.jobresult.status
            r = d.result
            begin = int(d.begin)
            end = int(d.end)
            if int(s):
                d.jobresult.status = '开启'
            else:
                d.jobresult.status = '关闭'
            if r == '1':
                d.result = '成功'
            if r == '2':
                d.result = '失败'
            if r == '3':
                d.result = '运行中'
            if begin:
                x = time.localtime(begin)
                d.begin = time.strftime('%Y-%m-%d %H:%M:%S', x)
            if end:
                x = time.localtime(end)
                d.end = time.strftime('%Y-%m-%d %H:%M:%S', x)
        return data
