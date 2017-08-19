#!//bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-07-27
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

mem_dic = {}
execute_pool = pool.Pool(2000)

URL = 'http://maps.google.cn/maps/api/geocode/json?latlng='

failed_list = []
complete_count = 0


from datetime import datetime
import functools
from collections import defaultdict
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



func_count_dict = defaultdict(int)

def func_time_logger(fun):
    if fun.__dict__.get('mioji.aop_utils.logger', False):
        return fun
    fun.__dict__['mioji.aop_utils.logger'] = True

    @functools.wraps(fun)
    def logging(*args, **kw):
        func_count_dict[fun.__name__] += 1
        begin = datetime.now()
        result = fun(*args, **kw)
        end = datetime.now()
        func_count_dict[fun.__name__] -= 1
        print 'func {0}, cost:{1}, ex:{2}'.format(fun.__name__, end - begin , func_count_dict[fun.__name__])
        return result

    return logging


@func_time_logger
def get_PROXY():
    R = requests.get('http://10.10.239.46:8087/proxy?user=crawler&passwd=spidermiaoji2014&source=google')
    p = R.content

    PROXY = {
        'http': 'socks5://' + p,
        'https': 'socks5://' + p
    }
    return PROXY


def get_repeat_sid(source):

    db = mysql_db_pool.connection()
    cursor = db.cursor()
    cursor.execute("select sid from hotel_unid where source='%s' group by sid having count(sid)>1" % source)
    sid_lis = cursor.fetchall()
    db.close()
    return sid_lis


@func_time_logger
def get_map_data(x, y):
    i = 0
    while i < 3:
        try:
            r = requests.get(URL + x + ',' + y, proxies=get_PROXY(), timeout=(5, 10))
            map_data = json.loads(r.content)
            return map_data['results'][0]['formatted_address']
        except:
            print 'get error'
            i += 1
    return None


@func_time_logger
def get_sid_hotels(source, sid):
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select mid , map_info,uid,name,name_en,hotel_url '
                'from hotel_unid where source = "%s" and sid = "%s"' % (source, sid))

    hotel_datas_list = cur.fetchall()
    db.close()

    return hotel_datas_list


@func_time_logger
def get_city_info(mid):
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select name_en,name,id,tri_code from city where id = "%s"' % mid)
    c_datas = cur.fetchall()[0]
    db.close()
    return c_datas


@func_time_logger
def match_city(source, sid):
    print 'start {0}'.format(sid)

    hotel_datas_list = get_sid_hotels(source, sid)
    map_info = hotel_datas_list[0][1].split(',')
    x, y = map_info[1], map_info[0]
    currect_name_en = get_map_data(x, y)#.encode('utf-8').strip()


    for hotel_datas in hotel_datas_list:
        mid = hotel_datas[0]
        map_info_lis = hotel_datas[1]
        uid = hotel_datas[2]
        name = hotel_datas[3]
        h_name_en = hotel_datas[4]
        hotel_url = hotel_datas[5]

        map_info_lis = []
        map_info_lis.append(x)
        map_info_lis.append(y)

        city_info = []
        if mem_dic.has_key(mid) == False:
            c_datas = get_city_info(mid)
            city_info.append(c_datas[0])
            city_info.append(c_datas[1])
            city_info.append(c_datas[2])
            city_info.append(c_datas[3])
            mem_dic[hotel_datas[0]] = city_info

        name_en = ''
        city_info = mem_dic[mid]
        name_en = city_info[0]

        jcity_info = json.dumps(city_info, ensure_ascii=False)
        jmap_info_lis = json.dumps(map_info_lis, ensure_ascii=False)
        ###
        if re.search('[ |,]'+str(name_en).lower()+'[ |,]',' '+str(currect_name_en).lower()+' ' ) and name_en != '' :
            dict_writer.writerow(
                {"source": source, "sid": sid, "uid": uid, "name": name, "name_en": h_name_en, "城市信息": jcity_info,
                 "API_adress": currect_name_en, "坐标": jmap_info_lis, "是否匹配": 'YES', "源URL": hotel_url})
        else:
            dict_writer.writerow(
                {"source": source, "sid": sid, "uid": uid, "name": name, "name_en": h_name_en, "城市信息": jcity_info,
                 "API_adress": currect_name_en, "坐标": jmap_info_lis, "是否匹配": 'NO', "源URL": hotel_url})
    global complete_count
    complete_count += 1
    print 'end {0}'.format(sid)
    print 'complete {0}'.format(complete_count)



if __name__ == '__main__':
    source = sys.argv[1]
    s = time.time()
    sid_lis = get_repeat_sid(source)

    csvFile = open("%s.csv" % source, "w")
    fileheader = ["source", "sid", "uid", "name", "name_en", "城市信息", "API_adress", "坐标", "是否匹配", "源URL"]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))

    cc = int(0)
    gs = []
    print len(sid_lis)
    for sid in sid_lis:
        sid = sid[0]
        g = execute_pool.apply_async(match_city, args=(source, sid))
        gs.append(g)

    gevent.joinall(gs)
    csvFile.close()
    print time.time() - s
