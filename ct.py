#-*- coding: utf-8 -*-
import logging
from logging.handlers import RotatingFileHandler
import os
import sys


reload(sys)
sys.setdefaultencoding('utf8')
import requesthandler
from flask import Flask

from Dac.DataSource import controltower_database, controltower_database_read_1
import scheduler
app = Flask(__name__)


requesthandler.init(app)


@app.teardown_request
def _db_close(exc):
    '''
    鍏抽棴mysql鏉冮檺
    :param exc:
    :return:
    '''
    if not controltower_database.is_closed():
        controltower_database.close()
    if not controltower_database_read_1.is_closed():
        controltower_database_read_1.close()


#########
# 记录错误日志
#########
def get_server_path():
    return os.path.split(os.path.realpath(__file__))[0]
logpath = os.path.join(os.path.dirname(get_server_path()), '_corp_platform_logs')
if not os.path.exists(logpath):
    os.mkdir(logpath)
    fp = open("error.log", 'w')
    fp.close()
errorlogpath = os.path.join(logpath, 'error.log')
errlogFormat = logging.Formatter(
    '%(process)d %(asctime)s %(levelname)s: %(message)s '
    '[in %(pathname)s:%(lineno)d]'
)
import platform

file_handler = RotatingFileHandler(errorlogpath, maxBytes=50000000, backupCount=5)
file_handler.setFormatter(errlogFormat)
file_handler.setLevel(logging.ERROR)
app.logger.addHandler(file_handler)

scheduler.reStart()

if __name__ == '__main__':
    # app.debug=True
    app.run()
