#!flask/bin/python
#-*- coding: utf-8 -*-
from Dac.DataSource import controltower_database, controltower_database_read_1

__author__ = 'xlliu'


from peewee import *

class UnknownField(object):
    pass

class BaseModel(Model):
    class Meta:
        database = controltower_database
        read_slaves = (controltower_database_read_1,)

class Job(BaseModel):
    command = CharField()
    name = CharField()
    cron = CharField()
    lastbegin = IntegerField()
    lastend = IntegerField()
    lastresult = CharField()
    status = CharField()

    class Meta:
        db_table = 'job'

class Log(BaseModel):
    begin = IntegerField()
    end = IntegerField()
    job = IntegerField(db_column='job_id')
    msg = TextField()
    result = CharField()

    class Meta:
        db_table = 'log'

