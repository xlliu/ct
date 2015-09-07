# encoding=utf-8
from playhouse.pool import PooledMySQLDatabase

__author__ = 'zhangjinglei'

from ConfigTool import getConnString
from peewee import *

controltower = getConnString('DataBase.controltower')
controltower_database = MySQLDatabase('controltower',
                                        # max_connections=8,
                                        # stale_timeout=300,
                                        threadlocals=True, **{'host': controltower.host,
                                                              'password': controltower.passwd,
                                                              'port': controltower.port,
                                                              'user': controltower.user,
                                                              'charset': controltower.charset})

controltower_read_1 = getConnString('DataBase.controltower.read.1')
controltower_database_read_1 = MySQLDatabase('controltower',
                                               # max_connections=8,
                                               # stale_timeout=300,
                                               threadlocals=True, **{'host': controltower_read_1.host,
                                                                     'password': controltower_read_1.passwd,
                                                                     'port': controltower_read_1.port,
                                                                     'user': controltower_read_1.user,
                                                                     'charset': controltower_read_1.charset})

