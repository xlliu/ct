# -*- coding: utf-8 -*-
import sys

from apscheduler.events import EVENT_ALL, EVENT_SCHEDULER_START, EVENT_JOB_ERROR
from Common.sendmessage import SendMessage
from Dac.DataSource import controltower_database
from Dal.controltower import Job, Log
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
import re
import traceback
from Common.common_utils import CommonUtils

__author__ = 'zhangjinglei'

# The "apscheduler." prefix is hard coded
scheduler = BackgroundScheduler({
    'apscheduler.executors.default': {
        'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
        'max_workers': '20',
    },
    'apscheduler.job_defaults.coalesce': 'false',
    'apscheduler.job_defaults.max_instances': '1',
})
scheduler_runing = False


def scheduler_listener(event):
    global scheduler_runing
    if event.code == EVENT_SCHEDULER_START:
        scheduler_runing = True
    elif event.code == EVENT_JOB_ERROR:
        event.traceback.format_exc()


scheduler.add_listener(scheduler_listener, EVENT_ALL)


def reStart():
    """
    重启整个scheduler
    :return:
    """
    # 重置scheduler
    if scheduler_runing:
        scheduler.shutdown(wait=False)
    # 重新添加各种job
    dbjobs = Job.select().where(Job.status == 1)
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
        scheduler.remove_job(str(id))


def addJob(item, reschedule=False):
    """
    增加/开启job
    :param item:
    :return:
    todo 是否已经存在了
    """
    if item.status == '1':
        if not scheduler.get_job(str(item.id)):
            scheduler.add_job(run, kwargs={"item": item, "cmd": item.command}, trigger='cron', id=str(item.id),
                              **getCron(item.cron))
        else:
            if reschedule:
                scheduler.reschedule_job(str(item.id), trigger='cron', **getCron(item.cron))
                print u'已重新定时'
            scheduler.modify_job(str(item.id), kwargs={"item": item, "cmd": item.command})
    else:
        if scheduler.get_job(str(item.id)):
            reMoveJob(item.id)
            print u'已经从列队中移除任务'
        else:
            print u'add updata列队中无此任务，不需要移除'
    return 'ok'


def run(item, cmd):
    stdout = u''
    stderr = u''
    begin = CommonUtils.get_unixtime()
    result = 1
    with controltower_database.execution_context() as ctx:
        # 修改任务状态为 开始运行
        try:
            Job.update(lastbegin=begin, lastend=0, lastresult=3).where(Job.id == str(item.id)).execute()
        except Exception, e:
            print e
        try:
            reload(sys)
            sys.setdefaultencoding('utf-8')
            child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = child.communicate()
        except Exception, e:
            print e
            stderr += traceback.format_exc()
        if stderr:
            print u'检测到异常,%s' %scheduler.get_jobs()
            print
            result = 2
            if item.phonenum:
                url = 'http://123.56.40.122:30003/sms/sendsms'
                content = "优办ct测试"
                # content = u"错错错，都是我的错"
                SendMessage.sendPhoneMessage(
                        url,
                        {"phone": '13351019032', "content": content},
                        {}
                )
            elif item.email:
                SendMessage().sendEmailMessage(
                        item.email.split(";"),
                        u'报表监控系统异常',
                        u'报表监控系统，%s脚本出现运行异常，请尽快关注' % item.name
                )

        end = CommonUtils.get_unixtime()
        # 修改任务状态为 结束运行
        Job.update(lastend=end, lastresult=result, runtime=end - begin).where(Job.id == str(item.id)).execute()
        # 记录脚本日志
        # print(stderr)
        try:
            stderr = stderr.decode('gbk').encode('utf-8')
            stdout = stdout.decode('gbk').encode('utf-8')
            Log.create(begin=begin, end=end, job=item.id,
                       msg=u'===============Print==========\n' + stdout + u'\n===============Error==========\n\n' + stderr,
                       result=result)
        except Exception, e:
            print e


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


def getjobnexttime(id):
    job = scheduler.get_job(str(id))
    if job:
        return job.next_run_time.strftime('"%Y-%m-%d %H:%M:%S"')

    return '--'
