# -*- coding: utf-8 -*-
import smtplib
import urllib
import urllib2
from email.mime.text import MIMEText

__author__ = 'xlliu'


class SendMessage(object):

    @staticmethod
    def sendPhoneMessage(url, data, headers):
        data = urllib.urlencode(data)
        req = urllib2.Request(url=url, data=data, headers=headers)
        response = urllib2.urlopen(req)
        return response.read()

    @staticmethod
    def sendEmailMessage(to_list, sub, content):  # to_list：收件人；sub：主题；content：邮件内容

        mail_host = "smtp.uban.com"
        mail_user = "liuxuelong@uban.com"
        mail_pass = "uban_2016"
        # 发件箱的后缀
        mail_postfix = "uban.com"
        # 这里的hello可以任意设置，收到信后，将按照设置显示
        me = "hello" + "<" + mail_user + "@" + mail_postfix + ">"
        # 创建一个实例，这里设置为html格式邮件
        msg = MIMEText(content, _subtype='html', _charset='gb2312')
        # 设置主题
        msg['Subject'] = sub
        # 来自
        msg['From'] = me
        # 目标
        msg['To'] = ";".join(to_list)
        try:
            s = smtplib.SMTP()
            # 连接smtp服务器
            s.connect(mail_host)
            # 登陆服务器
            s.login(mail_user, mail_pass)
            # 发送邮件
            s.sendmail(me, to_list, msg.as_string())
            s.close()
            return True
        except Exception, e:
            print str(e)
            return False

# list1 = '4123123@qq.com;dfasfs@qq.com;fdasfa@qq.com'.split(';')
# print list1
# print 2
# SendMessage().sendEmailMessage(list1, 'test', u'这是一个测试')