#!flask/bin/python
#-*- coding: utf-8 -*-
from Dal.controltower import Job

__author__ = 'xlliu'


class AddTaskDao(object):

    def addTask(self, name, status, command, cron):
        i = Job.create(name=name, status=status, command=command, cron=cron)
        return i