# coding=utf-8


__author__ = 'huaruibang'
import urllib2, urllib
import json
import os
import time
from datetime import datetime, timedelta


class CommonUtils(object):


    @staticmethod
    def getToday(days=0):
        d1 = datetime.now()
        d1 = datetime(d1.year ,d1.month,d1.day ,0,0,0)
        if days:
            d1 = d1 + timedelta(days = days)
        epoch = datetime(1970, 1, 1, hour=8)
        diff = d1 - epoch
        d = diff.days * 24 * 3600 + diff.seconds
        return d

    @staticmethod
    def unixtime_to_strYMDHMS(value):
        dt = datetime(1970, 1, 1,hour=8) + timedelta(seconds=value)
        dt = dt.strftime('%Y-%m-%d %H:%M:%S')
        # format = '%Y-%m-%d'
        # value为传入的值为时间戳(整形)，如：1332888820
        # value = time.localtime(value)
        ## 经过localtime转换后变成
        ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=0)
        # 最后再经过strftime函数转换为正常日期格式。
        # dt = time.strftime(format, value)
        return dt

    @staticmethod
    def unixtime_to_str(value):
        dt = datetime(1970, 1, 1, hour=8) + timedelta(seconds=value)
        dt = dt.strftime('%Y-%m-%d')
        # format = '%Y-%m-%d'
        # value为传入的值为时间戳(整形)，如：1332888820
        # value = time.localtime(value)
        ## 经过localtime转换后变成
        ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=0)
        # 最后再经过strftime函数转换为正常日期格式。
        # dt = time.strftime(format, value)
        return dt

    @staticmethod
    def str_to_unixtime(dt):
        d = datetime.strptime(dt, '%Y-%m-%d')

        epoch = datetime(1970, 1, 1, hour=8)
        diff = d - epoch
        d = diff.days * 24 * 3600 + diff.seconds
        # dt为字符串
        # 中间过程，一般都需要将字符串转化为时间数组
        # time.strptime(dt, '%Y-%m-%d')
        ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
        # 将"2012-03-28 06:53:40"转化为时间戳
        # s = time.mktime(time.strptime(dt, '%Y-%m-%d'))
        return int(d)

    @staticmethod
    def cnstr_to_unixtime(dt):
        d = None
        try:
            d = datetime.strptime(dt, '%Y年%m月%d日')
        except:
            try:
                d = datetime.strptime(dt, '%Y年%m月')
            except:
                d = datetime.strptime(dt, '%Y年')

        epoch = datetime(1970, 1, 1, hour=8)
        diff = d - epoch
        d = diff.days * 24 * 3600 + diff.seconds
        # dt为字符串
        # 中间过程，一般都需要将字符串转化为时间数组
        # time.strptime(dt, '%Y-%m-%d')
        ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
        # 将"2012-03-28 06:53:40"转化为时间戳
        # s = time.mktime(time.strptime(dt, '%Y-%m-%d'))
        return int(d)

    @staticmethod
    def datetime_to_unixtime(dt):

        epoch = datetime(1970, 1, 1, hour=8)
        diff = dt - epoch
        diff = diff.days * 24 * 3600 + diff.seconds
        # dt为字符串
        # 中间过程，一般都需要将字符串转化为时间数组
        # time.strptime(dt, '%Y-%m-%d')
        ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
        # 将"2012-03-28 06:53:40"转化为时间戳
        # s = time.mktime(time.strptime(dt, '%Y-%m-%d'))
        return int(diff)

    @staticmethod
    def get_unixtime():
        return int(time.time())

    @staticmethod
    def get_today_unixtime():
        '''
        获取日期 当前0点0分0秒的unixtime时间
        :return:
        '''
        today = datetime.today()
        t = (today.year, today.month, today.day, 0, 0, 0, 0, 0, 0)
        return int(time.mktime(t))

