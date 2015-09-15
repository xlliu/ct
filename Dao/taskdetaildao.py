#!flask/bin/python
#-*- coding: utf-8 -*-
import time
from Dal.controltower import Job
from Dao.taskbase import TaskBase

__author__ = 'xlliu'


class TaskDetailDao(TaskBase):

    def find_one(self, id):
        data = Job.select().where(Job.id == id)
        for d in data:
            s = d.status
            r = d.lastresult
            begin = int(d.lastbegin)
            end = int(d.lastend)
            if int(s):
                d.status = '开启'
            else:
                d.status = '关闭'
            if r == '1':
                d.lastresult = '成功'
            if r == '2':
                d.lastresult = '失败'
            if r == '3':
                d['lastresult'] = '运行中'
            if begin:
                x = time.localtime(begin)
                d.lastbegin = time.strftime('%Y-%m-%d %H:%M:%S', x)
            if end:
                x = time.localtime(end)
                d.lastend = time.strftime('%Y-%m-%d %H:%M:%S', x)
        return data

    def update_task(self, id, name, status, command, cron, photonum, email, errorkey):
        if id:
            oldcron=Job.get(Job.id==id).cron
            try:
                Job.update(name=name, status=status, command=command, cron=cron, photonum=photonum, email=email, errorkey=errorkey).where(Job.id == id).execute()
                self.updateTaskRun(id,oldcron)
            except Exception, e:
                print e
