1,自动生成models
===

    python -m pwiz -e mysql -H tianjixian.mysql.rds.aliyuncs.com -p 3306 -u zhangjinglei -P zjl12315 infrastructure

    python -m pwiz -e mysql -H tianjixian.mysql.rds.aliyuncs.com -p 3306 -u zhangjinglei -P zjl12315 corp_officebuilding

    python -m pwiz -e mysql -H tianjixian.mysql.rds.aliyuncs.com -p 3306 -u zhangjinglei -P zjl12315 reportdb

    
2,required:
---
    pip install peewee
    pip install PyMySQL
    pip install XlsxWriter

session required:
    pip install flask-session
    pip install python-binary-memcached
    python -m easy_install xlrd
    python -m pip install python-memcached

log:
    pip install raven
    pip install blinker