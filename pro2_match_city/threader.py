#-*- coding:UTF-8 -*-

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

execute_pool = pool.Pool(20)
URL = 'http://maps.google.cn/maps/api/geocode/json?latlng=32.2889670141,-106.737711132'
px = '10.10.233.246:34934'
P = {
        "http": "socks5://" + px,
        "https": "socks5://" + px

    }

def get_proxy():
    r = requests.get('http://10.10.239.46:8087/proxy?user=crawler&passwd=spidermiaoji2014&source=google')
    p = r.content

    PROXY = {
            'http':'socks5://'+p,
            'https':'socks5://'+p
            }
    return PROXY

def get_map_data():
    i = 0
    data=''
    while i<3:
        try:
            r = requests.get(URL,proxies= get_proxy())
            map_data = json.loads(r.content)
            data = map_data['results'][0]['formatted_address']
            break
        except:
            print 'get error'
            i+=1
    dic_writer.writerow({'坐标':'asd','城市信息':data})

if __name__ =='__main__':
    csvFile = open('test.csv','w')
    fileheader = ['坐标','城市信息']
    dic_writer = csv.DictWriter(csvFile,fileheader)
    dic_writer.writerow(dict(zip(fileheader,fileheader)))
    s = time.time()
    gs = []
    for i in range(10):
    
        g = execute_pool.apply_async(get_map_data,args=())
        gs.append(g)
    gevent.joinall(gs)
    print time.time()-s
    csvFile.close()
