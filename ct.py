#!flask/bin/python
#-*- coding: utf-8 -*-
import sys
import subprocess
import requesthandler

reload(sys)
sys.setdefaultencoding('utf8')
from flask import Flask

from Dac.DataSource import controltower_database, controltower_database_read_1
import scheduler
from Common.common_utils import CommonUtils
app = Flask(__name__)


requesthandler.init(app)


@app.teardown_request
def _db_close(exc):
    '''
    :param exc:
    :return:
    '''
    if not controltower_database.is_closed():
        controltower_database.close()
    if not controltower_database_read_1.is_closed():
        controltower_database_read_1.close()





scheduler.reStart()

if __name__ == '__main__':
    app.run(debug=True)
