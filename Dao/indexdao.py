#!flask/bin/python
#-*- coding: utf-8 -*-
import time
from Dal.controltower import Job

__author__ = 'xlliu'


class IndexDao(object):

    def taskList(self):
        data = Job.select()
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
                d.lastresult = '运行中'
            if begin:
                x = time.localtime(begin)
                d.lastbegin = time.strftime('%Y-%m-%d %H:%M:%S', x)
            if end:
                x = time.localtime(end)
                d.lastend = time.strftime('%Y-%m-%d %H:%M:%S', x)
        return data

    def del_task(self, id):
        if id:
            d = Job.delete().where(Job.id == id)
            d.execute()


