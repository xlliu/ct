# encoding=utf-8
__author__ = 'zhangjinglei'

from apscheduler.schedulers.background import BackgroundScheduler
import subprocess

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
    for item in jobs:
        scheduler.add_job(_job(item), 'cron', minutes=2, id=item.id)

    scheduler.start()

def reMoveJob(id):
    if scheduler.get_job(id):
        scheduler.remove_job(id)

def addJob(item):
    scheduler.add_job(_job(item), 'cron', minutes=2, id=item.id)

def _job(item):
    def _privatejob():
        child=subprocess.Popen(item.cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        stdout, stderr=child.communicate()
        print(stdout)
        print(stderr)
    return _privatejob