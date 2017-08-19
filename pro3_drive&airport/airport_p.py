#!//bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-08-18
@author: fengyufei
'''

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
import re
import math
import pymysql
from DBUtils.PooledDB import PooledDB

# db ip
base_ip = '10.10.69.170'
base_user = 'reader'
base_pwd = 'miaoji1109'
base_db = 'base_data'

mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)

country_abbr = {}
city_airport = {}


# distance
EARTH_RADIUS = 6378137
PI = 3.1415927

def rad(d):
    return d * PI / 180.0

def getDist(lng1, lat1, lng2, lat2):
    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)

    s = 2 * math.asin(
        math.sqrt(math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))

    s = s * EARTH_RADIUS
    s = round(s * 10000)/10000

    return int(s)


def getDistSimply(lng1, lat1, lng2, lat2):
    dx = lng1 - lng2
    dy = lat1 - lat2
    b = (lat1 + lat2) / 2.0
    lx = rad(dx) * EARTH_RADIUS * math.cos(rad(b))
    ly = EARTH_RADIUS * rad(dy)
    return int(math.sqrt(lx * lx + ly * ly))


def get_dist_by_map(map_1, map_2):
    try:
        return getDistSimply(float(map_1.split(',')[0]), float(map_1.split(',')[1]),
                             float(map_2.split(',')[0]), float(map_2.split(',')[1]))

    except Exception, e:
        print ('get_dist', ['map = ' + map_1 + '\t' + map_2])
        print map_2
        return 100000000000
#distence end


def get_city(cid):
    print cid
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select name,name_en,tri_code from city where id = {0}'.format(cid))
    air_datas = cur.fetchall()
    db.close()
    return air_datas[0]

def get_country(abbr):
    db = mysql_db_pool.connection()
    cur = db.cursor() 
    cur.execute('select name, name_en from country where mid = {0}'.format(abbr))
    db.close()
    return cur.fetchall()


def process_task():
    csvFile = open("airport_result.csv", "w")
    fileheader = ["cid", "mid","name","name_en","tri_code","city_map_info", "机场id", "机场名字", "英文名", "机场坐标", "belong_city_id","距离"]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    with open('airport.csv') as csvf:
        f_csv = csv.reader(csvf)
        h = next(f_csv)
        for row in f_csv:
            dist = get_dist_by_map(row[2],row[6])
            datas = get_city(row[0])
            dict_writer.writerow({"cid":row[0], "mid":row[1],"name":datas[0],"name_en":datas[1],"tri_code":datas[2], "city_map_info":row[2], "机场id":row[3], "机场名字":row[4], "英文名":row[5], "机场坐标":row[6], "belong_city_id":row[7],"距离":dist}) 


if __name__ == '__main__':
    process_task()
