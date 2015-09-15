# -*- coding: utf-8 -*-
import urllib2

__author__ = 'xlliu'


class SendMessage(object):

    @staticmethod
    def sendPhoneMessage(url, data, headers):
        req = urllib2.Request(url=url, data=data, headers=headers)
        response = urllib2.urlopen(req)
        return response.read()

    @staticmethod
    def sendEmailMessage():
        pass
