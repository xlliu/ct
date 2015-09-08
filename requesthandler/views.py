#!flask/bin/python
#-*- coding: utf-8 -*-
from flask import render_template, request, url_for
from flask.views import MethodView
from werkzeug.utils import redirect
from Dao.addtaskdao import AddTaskDao
from Dao.indexdao import IndexDao
from Dao.taskdetaildao import TaskDetailDao

__author__ = 'xlliu'
import scheduler


class GetIndex(MethodView):

    def get(self):
        data = IndexDao().taskList()
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
        cron = request.form.get('cron')
        job = AddTaskDao().addTask(name=name, status=status, command=command, cron=cron)
        scheduler.addJob(job)
        return redirect(url_for('/'))

class GetTaskDeatil(MethodView):

    def get(self):
        id = int(request.args.get('id'))
        data = TaskDetailDao().find_one(id)
        return render_template('task_detail.html', data=data)

    def post(self):
        id = request.form.get('id')
        name = request.form.get('tname')
        status = request.form.get('status')
        command = request.form.get('command')
        cron = request.form.get('cron')
        TaskDetailDao().update_task(id=id, name=name, status=status, command=command, cron=cron )
        return redirect(url_for('/'))

class DeleteTask(MethodView):

    def get(self):
        id = int(request.args.get('id'))
        IndexDao().del_task(id)
        return redirect(url_for('/'))
