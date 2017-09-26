#!//bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-08-18
@author: vassago
'''

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
import re
import math
import pymysql
from DBUtils.PooledDB import PooledDB
import multiprocessing
import time
import json
import requests
import multiprocessing
'''
#db 
psw
'''

mysql_db_admin = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)

mysql_db_reader = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip1, port=3306,                                                                                                                                
                        user=base_user1, passwd=base_pwd1, db=base_db1, charset='utf8', use_unicode=False, blocking=True)

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
    return int(math.sqrt(lx * lx + ly * ly))/1000   # --- m

def get_dist_by_map(map_1, map_2):
    try:
        return getDistSimply(float(map_1.split(',')[0]), float(map_1.split(',')[1]),
                             float(map_2.split(',')[0]), float(map_2.split(',')[1]))

    except Exception, e:
        print ('get_dist', ['map = ' + map_1 + '\t' + map_2])
        print map_2
        return 100000000000
#distence end

def get_PROXY():
    R = requests.get('url')
    p = R.content
    if p.split('.')[0] == '10':
        PROXY = {
            'http': 'socks5://' + p,
            'https': 'socks5://' + p
        }
    else:
        PROXY = {
            'http': 'http://' + p,
            'https': 'https://' + p
        }
    return PROXY

def get_datas_from_file(fname):
    def get_data_from_csv(fname):
        with open(fname) as f:
            final= []
            f_csv = csv.reader(f)
            headers = next(f_csv)
            print headers
            for row in f_csv:
                final.append(row)
            return final
    def get_data_from_forml(fname):
        with open(fname) as f:
            final = []
            datas = f.readlines()
            for data in datas:
                final.append(data.replace('\n',''))
            return final

    if fname.find('.')>0:
        if fname.split('.')[-1]=='csv':
            return get_data_from_csv(fname)
        else:
            return get_data_from_forml(fname)

def process_data(mdata):
    def process_data(mdata):
    dict_writer.writerow({"name":mdata[0],'name_en':mdata[1],'country':mdata[2],'suggestion':mdata[3]})
    print '-over--->',mdata[0]

def insert_db(data):
    db = mysql_db_admin.connection()
    cur = db.cursor()
    sql = """INSERT INTO hotel_suggestions_city(
    city_id, source, suggestions, select_index,annotation
    )
    VALUES (%s,%s,%s,%s,%s)"""
    try:
        cur.execute(sql,(data[0],data[1],data[2],data[3]))
        print '--OK--'
        db.commit()
    except Exception as e:
        print '--err-',e
        db.rollback()

    db.close()

def encode_unicode(str):
    return str.replace('\u00', '\\x').decode('string-escape').encode('utf8')

def encode_text(str):
    lis = re.split('[ \n\t ]',str)
    liss = filter(lambda x: x!='', lis)
    return liss

def my_re(string):
    china_li = re.findall('[\x80-\xff]+',string)

def request_suggestion(url, headers = None):
    i = 1
    while i < 5:
        try:
            r = requests.get(url,headers = headers,proxies = get_PROXY())
            suggestion = json.loads(r.content)
            return suggestion
        except Exception as e:
            print e,'get-error'
            i += 1
    return None

def get_suggestions_elong(url):
    city_list = get_datas_from_file('booking_city_list.csv')
    all_ids =[]

    def task(city):
        ul = url.format(city[-2])
        suggestion = request_suggestion(ul)['data']
        index = -1
        print '--process-->',city[-2]
        if suggestion != None and suggestion.has_key('city'):
            for sug in suggestion['city']:
                if all_ids.count(sug['id'])== 0:
                    all_ids.append(sug['id'])
                    data = [sug['name_cn'],sug['name_en'],sug["region_info"]['country_name_cn'],sug]
                    process_data(data)
    
    gs = []
    for city in city_list:                             
        g = execute_pool.apply_async(task, args=(city,))
        gs.append(g)
    gevent.joinall(gs)

def process(args):
    '''
    work
    '''
    return result

if __name__ == '__main__':
    '''
    csvFile = open("test.csv", "w")
    fileheader = ["name","country","id","name_en" ]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    match_city_with_db('hilton_city_list0.csv')
    '''

    pool = multiprocessing.Pool(processes = 100)
    for t in tasks:
        pool.apply_async(process, (t, ),callback=write_file)
    pool.close()
    pool.join()
