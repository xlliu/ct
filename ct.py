from flask import Flask,render_template
from plan import Plan
app = Flask(__name__)

from CronTabUtil import CronTab,Matcher

entry = CronTab('*/80 * * * *')
print(entry.next())

@app.route('/')
def hello_world():
    return render_template('index.html')


if __name__ == '__main__':
    app.run()
