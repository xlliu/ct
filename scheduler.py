#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
from apscheduler.events import EVENT_ALL, EVENT_SCHEDULER_START
from Dal.controltower import Job, Log

__author__ = 'zhangjinglei'

from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
import re
import traceback
from Common.common_utils import CommonUtils




# The "apscheduler." prefix is hard coded
scheduler = BackgroundScheduler({
    'apscheduler.executors.default': {
        'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
        'max_workers': '20',
    },
    'apscheduler.job_defaults.coalesce': 'false',
    'apscheduler.job_defaults.max_instances': '10',
})
scheduler_runing = False


def scheduler_listener(event):
    global scheduler_runing
    if event.code == EVENT_SCHEDULER_START:
        scheduler_runing = True


scheduler.add_listener(scheduler_listener, EVENT_ALL)

jobs = {}


def reStart():
    """
    重启整个scheduler
    :return:
    """
    # 重置scheduler
    if scheduler_runing:
        print 2
        scheduler.shutdown(wait=False)
    # 重新添加各种job
    dbjobs = Job.select().where(Job.status == 1)
    jobs.clear()
    for dbjob in dbjobs:
        addJob(dbjob)
    scheduler.start()


def reMoveJob(id):
    """
    关闭和删除job
    :param id:
    :return:
    """
    if scheduler.get_job(str(id)):
        jobs.pop(str(id), '')
        scheduler.remove_job(str(id))


def addJob(item):
    """
    增加/开启job
    :param item:
    :return:
    todo 是否已经存在了
    """
    if item.status == '1':
        if not scheduler.get_job(str(item.id)):
            jobs[str(item.id)] = item.id
            scheduler.add_job(_job(item), 'cron', id=str(item.id), **getCron(item.cron))

        # if not scheduler.get_job(str(item.id)):
        #     jobs[str(item.id)]=item
        #     scheduler.add_job(_job(str(item.id)), 'cron',id=str(item.id), **getCron(item.cron))
        else:
            print 1
            scheduler.reschedule_job(str(item.id), trigger='cron', **getCron(item.cron))

    else:
        if scheduler.get_job(str(item.id)):
            reMoveJob(item.id)
        else:
            print 'add updata列队中无此任务，不需要移除2'


def _job(item):
    def run():
        stdout = ''
        stderr = ''
        begin = CommonUtils.get_unixtime()
        result = 1
        # 修改任务状态为 开始运行
        Job.update(lastbegin=begin, lastend=0, lastresult=3).where(Job.id == item.id).execute()
        try:
            child = subprocess.Popen(item.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = child.communicate()

        except Exception, ex:
            stderr = traceback.format_exc()

        if stderr:
            result = 2

        end = CommonUtils.get_unixtime()
        # 修改任务状态为 结束运行
        Job.update(lastend=end, lastresult=result, runtime=end - begin).where(Job.id == item.id).execute()
        # 记录脚本日志
        Log.create(begin=begin, end=end, job=item.id,
                   msg='===============Print==========\n' + stdout + '\n===============Error==========\n\n' + stderr,
                   result=result)

    return run


def getCron(cronstr):
    cronstr = re.sub('\s+', ' ', cronstr)
    items = cronstr.split(' ')
    if len(items) != 5:
        raise Exception, 'cron不正确'
    return {'minute': items[0],
            'hour': items[1],
            'day': items[2],
            'month': items[3],
            'day_of_week': items[4]
            }
