# -*- coding: utf-8 -*-
import os
import sys
import xlsxwriter

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'xlliu'

import xlrd
# data = xlrd.open_workbook('import_floor_work.xlsx')
# table = data.sheet_by_index(0)
# nrows = table.nrows
# print nrows
# for i in xrange(1,nrows):
#     print table.row_values(i)[0]
#
# workbook = xlsxwriter.Workbook('c://xlliu_writer.xlsx')
# worksheet = workbook.add_worksheet()
# worksheet.write(3, 3, unicode('城市', 'utf-8'))
# workbook.close()

# cmd = 'python c:/new_customerandfollow_p.py 2015-10-02 2015-10-16 2015-10-02--2015-10-16 c:/'
#
# # cmd = r'python c:\\new_1.py 2015-10-02 2015-10-16 2015-10-02--2015-10-16 c:\\'
# import subprocess
# p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#
# stdout,stderr = p.communicate()
# print 'stdout : ',stdout
# print 'stderr : ',stderr
# # os.popen(cmd)
# print 'ok'

a=RawQuery(Houseinfo,"""
SELECT
  *
FROM
  `corp_officebuilding`.`houseinfo`
  JOIN
  corp_management.`user`
  ON corp_management.`user`.`user_id`=`corp_officebuilding`.`houseinfo` .accendantid
  where `corp_officebuilding`.`houseinfo` .accendantid=%s
LIMIT 0, 1000 ;
""",19817).dicts()
for one in a:
    print(one)
