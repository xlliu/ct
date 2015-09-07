#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
from flask import Flask,render_template

from Dac.DataSource import controltower_database, controltower_database_read_1
import scheduler
from Common.common_utils import CommonUtils
app = Flask(__name__)





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


@app.route('/')
def hello_world():
    return render_template('index.html')
print(CommonUtils.cronToNextTime('50 20 * * *'))
scheduler.reStart()

if __name__ == '__main__':
    app.run()
