# encoding=utf-8
import subprocess

__author__ = 'zhangjinglei'
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
child = subprocess.Popen('ls', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
(stdout, stderr) = child.communicate()
stderr = stderr.decode("gb2312")
print stderr
