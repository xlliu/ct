#!flask/bin/python
#-*- coding: utf-8 -*-
from flask import render_template, request, url_for
from flask.views import MethodView
from werkzeug.utils import redirect
from Common.common_utils import CommonUtils
from Dao.addtaskdao import AddTaskDao
from Dao.indexdao import IndexDao
from Dao.showlogdao import ShowLogDao
from Dao.taskdetaildao import TaskDetailDao

__author__ = 'xlliu'

import scheduler

class GetIndex(MethodView):

    def get(self):
        data = IndexDao().taskList()
        for d in data:
            d.nextruntime = scheduler.getjobnexttime(d.id)

        return render_template('index.html', data=data)


class GetDeatil(MethodView):

    def get(self):
        return render_template('detail.html')


class GetAddTask(MethodView):

    def get(self):
        return render_template('add_task.html')

    def post(self):
        name = request.form.get('tname')
        status = request.form.get('status')
        command = request.form.get('command')
        cronlist = []
        cronlist.append(request.form.get('cron1'))
        cronlist.append(request.form.get('cron2'))
        cronlist.append(request.form.get('cron3'))
        cronlist.append(request.form.get('cron4'))
        cronlist.append(request.form.get('cron5'))
        phonenum = request.form.get('phonenum', u'')
        email = request.form.get('email', u'')
        errerkey = request.form.get('errerkey', u'')
        cron = CommonUtils.cronStr(cronlist)
        AddTaskDao().addTask(name=name, status=status, command=command, cron=cron,
                             phonenum=phonenum, email=email, errerkey=errerkey)
        return redirect(url_for('/'))


class GetTaskDeatil(MethodView):

    def get(self):
        id = int(request.args.get('id'))
        data = TaskDetailDao().find_one(id)
        data[0].cron = data[0].cron.split()

        return render_template('task_detail.html', data=data)

    def post(self):
        id = request.form.get('id')
        name = request.form.get('tname')
        status = request.form.get('status')
        command = request.form.get('command')
        cronlist = []
        cronlist.append(request.form.get('cron1'))
        cronlist.append(request.form.get('cron2'))
        cronlist.append(request.form.get('cron3'))
        cronlist.append(request.form.get('cron4'))
        cronlist.append(request.form.get('cron5'))
        cron = CommonUtils.cronStr(cronlist)
        phonenum = request.form.get('photonum', u'')
        email = request.form.get('email', u'')
        errerkey = request.form.get('errerkey', u'')
        result = TaskDetailDao().update_task(id=id, name=name, status=status, command=command, cron=cron,
                                    phonenum=phonenum, email=email, errerkey=errerkey)
        if result:
            return redirect(url_for('/'))


class DeleteTask(MethodView):

    def get(self):
        id = int(request.args.get('id'))
        IndexDao().del_task(id)
        return redirect(url_for('/'))


class ShowLog(MethodView):

    def get(self):
        data = ShowLogDao().showLog(None)
        return render_template('show_log.html', data=data)


class GetLog(MethodView):

    def get(self):
        id = request.args.get('id')
        data = ShowLogDao().showLog(id)
        return render_template('show_log.html', data=data)

