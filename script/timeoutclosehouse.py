# coding=utf-8
import mysql.connector

config = {'host': 'tianjixian.mysql.rds.aliyuncs.com',
          'user': 'zhangjinglei',
          'password': 'zjl12315',
          'port': 3306,
          'database': 'corp_officebuilding',
          'charset': 'utf8'
          }
try:
    cnn = mysql.connector.connect(**config)
except mysql.connector.Error as e:
    print('connect fails!{}'.format(e))

cursor = cnn.cursor()
try:
    sql_query='SELECT id,confirmcloseid,confirmclosename FROM houseinfo WHERE confirmclosetime < UNIX_TIMESTAMP() - 3600*2 AND confirmclosetime <> 0'
    cursor.execute(sql_query)
    value = cursor.fetchall()
    if len(value) > 0:
        for i in value:
            cursor.execute('update houseinfo set status = status+10 where id = %s', [i[0]])
            cnn.commit()
            cursor.execute('insert into houseinfo_baseinfo_changlog (houseid, create_at, creator, creator_name, content) values (%s,UNIX_TIMESTAMP(),%s,%s,%s)', [i[0], i[1], i[2], '自动关闭'])
            cnn.commit()
except mysql.connector.Error as e:
  print('query error!{}'.format(e))
finally:
  cursor.close()
  cnn.close()
