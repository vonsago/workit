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
'''
psw
'''

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
    db.close()
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
    print target_city
    return target_city

def get_city():
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select id,map_info,country_id from city')
    air_datas = cur.fetchall()
    db.close()
    return air_datas

def match_city_abbr():
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select name,name_en from country')
    datas = cur.fetchall()
    #('\xe4\xb8\xad\xe5\x9b\xbd', '101', 'China')
    for data in datas:
        country_abbr[data[0]]= data[1]
        country_abbr[data[2]]= data[1]
    db.close()

def change_map(map_info):
    try:
        li = map_info.split(',')
        a = li[1]+','+li[0]
        return a
    except:
        return ''

def get_country(abbr):
    db = mysql_db_pool.connection()
    cur = db.cursor() 
    cur.execute('select name, name_en from country where mid = {0}'.format(abbr))
    db.close()
    return cur.fetchall()

def process_airport(mid,bcid):
    country_info = get_country(mid)
    country_name = country_info[0][0]
    country_name_en  = country_info[0][1]

    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select id,name,name_en,belong_city_id,map_info,country,inner_order,status from airport where country = "{0}" or country = "{1}" or belong_city_id = {2}'.format(country_name,country_name_en,bcid))
    air_lis = list(cur.fetchall())
    return air_lis

def get_trans_degree(cid):
    if cid =='NULL':
        return -1
    db = mysql_db_pool.connection() 
    cur = db.cursor() 
    cur.execute('select trans_degree from city where id = {0}'.format(cid))
    degree = cur.fetchall()
    db.close()
    try:
        return degree[0][0]
    except:
        return -1

def process_airport_list(city,airport_list):
    pre_airport_list,pre = [],[]
    result = {}
    city_map_info = change_map(city[1])
    print city[1]
    if city_map_info =='':
        print '--==none1==--'
        result[city] =None
        return result
    print '1L',len(airport_list)

    for air in airport_list:
        if city[1] == 'NULL' or air[4] == 'NULL':
            continue
        dis = get_dist_by_map(change_map(city[1]),change_map(air[4]))
        if dis <= 100000:
            pre_airport_list.append(air)

    print '2L',len(airport_list)

    if (len(pre_airport_list)==0):
        print '--==none2==--'
        result[city]=None
        return result
    if len(pre_airport_list)==1:
        result[city]=pre_airport_list[0]
        return result
    #
    g1,g2,g3 = [],[],[]
    for air in pre_airport_list:
        if get_trans_degree(air[3]) == 1:
            g1.append(air)
        elif get_trans_degree(air[3]) == 0:
            g2.append(air)
        else:
            g3.append(air)
    if len(g1)>0:
        pre = g1
    elif len(g2)>0:
        pre = g2
    else:
        pre = g3
    
    print '3L',pre

    if len(pre)==1:
        result[city]=pre[0]
        return result
    #
    g1,g2,g3 = [],[],[]
    for p in pre:
        if p[6] == 1:
            g1.append(p)
        elif p[6] == 0:
            g2.append(p)
        else:
            g3.append(p)
    if len(g1)>0:
        pre = g1
    elif len(g2)>0:
        pre = g2
    else:
        pre = g3
    
    print '4L',pre

    if len(pre) == 1:
        result[city]=pre[0]
        return result
    #
    mindis = 100000
    index = 0 
    for i in range(len(pre)):
        print pre[i]
        dis = get_dist_by_map(change_map(pre[i][4]),city_map_info) 
        if dis< mindis:
            mindis = dis
            index = i
    result[city]= pre[index]
    return result


def process_city():
    target_city = get_target_city(get_city())
    print len(target_city)
    #('60143', '-75.725555,-14.068056', '711')
    #(172, 'name1', 'Eu2', '10068', '7.529118,47.598736', '\xe7\x91\x9e\xe5\xa3\xab', 1, 'Open')
    for city in target_city:
        cid = city[0]
        cmap_info =city[1]
        mid = city[2]
        airport_list = process_airport(mid,cid)
        result = process_airport_list(city,airport_list)
        data = result[city]
        try:
            dict_writer.writerow({"cid":cid, "mid":mid, "city_map_info":cmap_info, "机场id":data[0], "机场名字":data[1], "英文名":data[2], "机场坐标":data[4], "belong_city_id":data[3]}) 
        except:
            err = 'error'
            dict_writer.writerow({"cid":cid, "mid":mid, "city_map_info":cmap_info, "机场id":err, "机场名字":err, "英文名":err, "机场坐标":err, "belong_city_id":err}) 


if __name__ == '__main__':
    csvFile = open("airport.csv", "w")
    fileheader = ["cid", "mid", "city_map_info", "机场id", "机场名字", "英文名", "机场坐标", "belong_city_id"]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    #dict_writer.writerow() 

    process_city() 

