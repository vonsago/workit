#!//bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-08-03
@author: fengyufei
'''

from gevent import monkey
monkey.patch_all()

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import requests
import json
import time
import gevent
from gevent import pool
import re

import pymysql
from DBUtils.PooledDB import PooledDB

# db ip
'''
db psw
'''

mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)

mem_dic = {}
execute_pool = pool.Pool(2000)

URL = 'http://maps.google.cn/maps/api/geocode/json?latlng='

target_url = 'http://maps.google.cn/maps/api/directions/json?origin=%s&destination=%s&mode=driving'

def get_city_data(cid):
	db = mysql_db_pool.connecton()
	cursor = db.cursor()
	cursor.execute('select map_info from city where id =%s'%cid)
	map_info = cursor.fetchall()
	print map_info
	db.close()

if __name__ == '__main__':
	print sys.argv
	
