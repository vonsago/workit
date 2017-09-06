#!//bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-08-31
@author: fengyufei
'''
from gevent import monkey
monkey.patch_all()

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
import re
import requests
import pymysql
from DBUtils.PooledDB import PooledDB
import multiprocessing
import time
import json

import gevent
from gevent import pool

# db ip
base_ip = '10.10.69.170'
base_user = 'reader'
base_pwd = 'miaoji1109'
base_db = 'base_data'

mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)

execute_pool = pool.Pool(300)
URL ='http://maps.google.cn/maps/api/geocode/json?address={}'


def get_PROXY():
    R = requests.get('http://10.10.239.46:8087/proxy?user=crawler&passwd=spidermiaoji2014&source=google')
    p = R.content
    PROXY = {
            'http': 'socks5://' + p,
            'https': 'socks5://' + p
            }
    return PROXY

def get_city():
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select id,name,name_en,tri_code,region,map_info,country_id from city')
    datas = cur.fetchall()
    db.close()
    return datas

def get_country_name(cid):
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select name from country where mid = "{}"'.format(cid))
    name = cur.fetchall()[0][0]
    return name

def re_request(datas,name,region,country):
    print '-----proces id= ',datas[0]
    url =URL.format(datas[2])
    i =0
    while i <5:
        try:
            r = requests.get(url, proxies=get_PROXY(), timeout=(5, 10))
            api_data = r.content
            process_datas(datas,api_data,country)
            break
        except:
            print '----get error'
            i+=1
            if i == 5:
                f.write(datas[0])
                f.write('\n')

def request_api(datas,name,region,country):
    print 'proces id= ',datas[0]
    if region =='NULL':
        que = ','.join([name,country])
    else:
        que = ','.join([name,region,country])
    url = URL.format(que)
    i = 0
    while i<5:
        try:
            r = requests.get(url, proxies=get_PROXY(), timeout=(5, 10))
            api_data = r.content
            process_datas(datas,api_data,country)
            break
        except:
            i += 1
            if i==5:
                print 'retry in english'
                re_request(datas,name,region,country)

def process_datas(db_datas,api_datas,coun):
    final = list(db_datas[:-1])
    final.append(coun)
    dic = json.loads(api_datas)
    formatted_address = dic['results'][0]['formatted_address']
    final.append(formatted_address)
    final.append(api_datas)

    return_file(final)


def return_file(data):
    try:
        dict_writer.writerow({"id":data[0],"name":data[1], "name_en":data[2], "tri_code":data[3],"region":data[4],"map_info":data[5] ,"country":data[6],"formatted_address":data[7],"API_adress":data[8]} )
    except Exception as e:
        print e

def process_task():
    citys = get_city()
    gs = []
    #('10001', '\xe5\xb7\xb4\xe9\xbb\x8e', 'Paris', 'PAR', 'NULL', '2.351492339147967,48.85746107178952','mid')
    for city in citys:
        country = get_country_name(city[6])
        g = execute_pool.apply_async(request_api, args=(city,city[1],city[4],country,))
        gs.append(g)
    gevent.joinall(gs)


if __name__ == '__main__':
    s = time.time()
    f = open('err','w')
    csvFile = open("google_city.csv", "w")
    fileheader = ["id", "name", "name_en", "tri_code","region","map_info" ,"country","formatted_address","API_adress"]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    process_task()
    csvFile.close()
    f.close()
    print time.time()-s
