#!flask/bin/python
#-*- coding: utf-8 -*-
from Dal.controltower import Job
from scheduler import addJob, reMoveJob

__author__ = 'xlliu'

class TaskBase(object):

    def addTaskRun(self, name, status, command, cron, phonenum, email, errerkey):
        s = Job.select().where(Job.name==name,Job.status==status,Job.command==command,
                               Job.cron==cron,Job.photonum==phonenum,
                               Job.email==email, Job.errorkey==errerkey)
        addJob(s[0])

    def updateTaskRun(self, id,cron):
        s = Job.select().where(Job.id==id)
        addJob(s[0],reschedule=s[0].cron.strip()!=cron.strip())

    def delTaskRun(self, id):
        reMoveJob(id)
