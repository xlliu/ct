# encoding=utf-8
import sys

reload(sys)
sys.setdefaultencoding('utf8')
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
        'max_workers': '20'
    },
    'apscheduler.job_defaults.coalesce': 'false',
    'apscheduler.job_defaults.max_instances': '1'

})

jobs={}
def reStart():
    """
    重启整个scheduler
    :return:
    """
    dbjobs = Job.select().where(Job.status==1)
    for dbjob in dbjobs:
        addJob(dbjob)
    scheduler.start()


def reMoveJob(id):
    """
    关闭和删除job
    :param id:
    :return:
    """
    if id in jobs:
        id=str(id)
        if scheduler.get_job(id):
            jobs.pop(int(id))
            scheduler.remove_job(id)
    else:
        print 'del列队中无此任务，无需移除'


def addJob(item):
    """
    增加/开启job
    :param item:
    :return:
    todo 是否已经存在了
    """
    if item.status=='1':
        if item.id not in jobs:
            jobs[item.id]=item.id
            scheduler.add_job(_job(item), 'cron',id=str(item.id), **getCron(item.cron))
        else:
            print 'add updata列队里已经有此任务，不需要添加'
    else:
        if item.id in jobs:
            reMoveJob(item.id)
        else:
            print 'add updata列队中无此任务，不需要移除'

def _job(item):
    def _privatejob():
        stdout=''
        stderr=''
        begin = CommonUtils.get_unixtime()
        result = 1
        # 修改任务状态为 开始运行
        Job.update(lastbegin = begin,lastend =0,lastresult = 3).where(Job.id==item.id).execute()
        try:
            child=subprocess.Popen(item.command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout, stderr=child.communicate()

        except Exception,ex:
            stderr = traceback.format_exc()

        if stderr:
            result=2

        end = CommonUtils.get_unixtime()
        # 修改任务状态为 结束运行
        Job.update(lastend =end,lastresult = result).where(Job.id==item.id).execute()
        # 记录脚本日志
        Log.create(begin =begin,end = end,job = item.id,msg = '===============Print==========\n'+stdout+'\n===============Error==========\n\n'+stderr,result = result)

    return _privatejob

def getCron(cronstr):
    cronstr = re.sub('\s+',' ',cronstr)
    items=cronstr.split(' ')
    if len(items)!=5:
        raise Exception,'cron不正确'
    return {'minute':items[0],
            'hour':items[1],
            'day':items[2],
            'month':items[3],
            'day_of_week':items[4]
            }
