#!flask/bin/python
#-*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
from flask import Flask,render_template
from Dac.DataSource import controltower_database, controltower_database_read_1

app = Flask(__name__)


@app.teardown_request
def _db_close(exc):
    '''
    关闭mysql权限
    :param exc:
    :return:
    '''
    if not controltower_database.is_closed():
        controltower_database.close()
    if not controltower_database_read_1.is_closed():
        controltower_database_read_1.close()


@app.route('/')
def hello_world():
    print 'b'
    return render_template('index.html')

@app.route('/detail')
def getDeatil():
    print 'a'
    return render_template('detail.html')




if __name__ == '__main__':
    app.run(debug=True)
