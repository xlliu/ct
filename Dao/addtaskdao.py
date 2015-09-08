#!flask/bin/python
#-*- coding: utf-8 -*-
from Dal.controltower import Job
from Dao.taskbase import TaskBase

__author__ = 'xlliu'


class AddTaskDao(TaskBase):

    def addTask(self, name, status, command, cron):
        Job.insert(name=name, status=status, command=command, cron=cron).execute()
        self.addTaskRun(name, status, command, cron)
