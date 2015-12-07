#!flask/bin/python
# -*- coding: utf-8 -*-
from Dal.controltower import Job
from Dao.taskbase import TaskBase

__author__ = 'xlliu'


class AddTaskDao(TaskBase):
    def addTask(self, name, status, command, cron, phonenum='', email='', errerkey=''):
        try:
            Job.insert(name=name, status=status, command=command, cron=cron,
                       phonenum=phonenum, email=email, errerkey=errerkey).execute()
        except Exception, e:
            print e
        self.addTaskRun(name, status, command, cron, phonenum, email, errerkey)
