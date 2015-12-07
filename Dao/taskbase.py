#!flask/bin/python
# -*- coding: utf-8 -*-
from Dal.controltower import Job
from scheduler import addJob, reMoveJob

__author__ = 'xlliu'


class TaskBase(object):
    def addTaskRun(self, name, status, command, cron, phonenum, email, errerkey):
        try:
            s = Job.select().where(Job.name == name, Job.status == status, Job.command == command,
                                   Job.cron == cron, Job.phonenum == phonenum,
                                   Job.email == email, Job.errerkey == errerkey)
            addJob(s[0])
        except Exception, e:
            print e

    def updateTaskRun(self, id, cron):
        s = Job.select().where(Job.id == id)
        return addJob(s[0], reschedule=s[0].cron.strip() != cron.strip())

    def delTaskRun(self, id):
        reMoveJob(id)
