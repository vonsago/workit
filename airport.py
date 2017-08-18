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
    s = round(s * 10000) / 10000

    return int(s)


def getDistSimply(lng1, lat1, lng2, lat2):
    dx = lng1 - lng2
    dy = lat1 - lat2
    b = (lat1 + lat2) / 2.0
    lx = rad(dx) * EARTH_RADIUS * math.cos(rad(b))
    ly = EARTH_RADIUS * rad(dy)
    return int(math.sqrt(lx * lx + ly * ly))/1000


def get_dist_by_map(map_1, map_2):
    try:
        return getDistSimply(float(map_1.split(',')[0]), float(map_1.split(',')[1]),
                             float(map_2.split(',')[0]), float(map_2.split(',')[1]))

    except Exception, e:
        print ('get_dist', ['map = ' + map_1 + '\t' + map_2])
        print map_2
        return 100000000000
#distence end

def get_airport():
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select id,name,name_en,city_id,belong_city_id,map_info,inner_order,status from airport')
    c_datas = cur.fetchall()
    return c_datas

def get_target_city(c_datas):
    air_data = get_airport()
    target_city = []
    air_list=[]
    for a in air_data:
        air_list.append(a[3])
    for c in c_datas:
        if air_list.count(c[0])==0:
            target_city.append(c)
    return target_city

def get_city():
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select id,map_info,country_abbr from city')
    air_datas = cur.fetchall()
    return air_datas

def match_city_abbr():
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select name, shor_name_cn form country')
    datas = cur.fetchall()
    for data in datas:
        country_abbr[data[1]]=data[0]

def process_null_airport():
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select id,name,name_en,city_id,belong_city_id,map_info,country,inner_order,status from airport where belon_city_id ="NULL"')
    null_airport = cur.fetchall()

    match_city_abbr()

    for air in null_airport:
        country = air[6]


    db.close()


def process_city():
    target_city = get_target_city(get_city())
    
    db = mysql_db_pool.connection()
    cur = db.cursor()
    for city in target_city:
        cur.execute('select id,name,name_en,city_id,belong_city_id,map_info,inner_order,status from airport where belong_city_id = {0} or belong_city_id = "NULL"'.format(city[0]))
        airport = cur.fetchall()

    db.close()
if __name__ == '__main__':
    

    '''
    csvFile = open("airport.csv", "w")
    fileheader = ["source", "sid", "uid", "name", "name_en", "城市信息", "API_adress", "坐标", "是否匹配", "源URL"]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    dict_writer.writerow(
                {"source": source, "sid": sid, "uid": uid, "name": name, "name_en": h_name_en, "城市信息": jcity_info,
                 "API_adress": currect_name_en, "坐标": jmap_info_lis, "是否匹配": 'YES', "源URL": hotel_url})
    '''


