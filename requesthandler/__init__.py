#!flask/bin/python
#-*- coding: utf-8 -*-
from requesthandler.views import GetIndex, GetDeatil, GetAddTask, GetTaskDeatil, DeleteTask

__author__ = 'xlliu'

def init(app):
    app.add_url_rule("/", view_func=GetIndex.as_view('/'))
    app.add_url_rule("/detail", view_func=GetDeatil.as_view('datail'))
    app.add_url_rule("/add_task", view_func=GetAddTask.as_view('add_task'))
    app.add_url_rule("/task_detail", view_func=GetTaskDeatil.as_view('task_detail'))
    app.add_url_rule("/delete_task", view_func=DeleteTask.as_view('delete_task'))
