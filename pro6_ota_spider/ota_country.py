#!//bin/python
# -*- coding: UTF-8 -*-

'''
@date : 2017-08-18
@author: vassago 
@update: 2017-09-21
'''

from gevent import monkey
monkey.patch_all()

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
import re
import math
import multiprocessing
import time
import json
import requests
import gevent
from gevent import pool

def get_PROXY():
    R = requests.get('url')
    p = R.content
    PROXY = {
            'http': 'socks5://' + p,
            'https': 'socks5://' + p
            }
    return PROXY


def request_suggestion(url):
    i = 0
    while i < 3:
        try:
            r = requests.get(url,proxies=get_PROXY(),timeout=(5, 10))
            sugg = json.loads(r.content)
            return sugg
        except:
            print 'get error'
            i += 1
            return None

def insert_all_db():
    datas =get_datas_from_file('hilton_city_list1.csv')
    for data in datas:
        if data[-1]=='NO':
            continue
        if data[1]=='中国':
            r = requests.get(url.format(data[0]))
        else:
            r = requests.get('http://www.hilton.com.cn/Handler/AutoComplete.ashx?type=get&nation=1&chinese=1&q={}&limit=500&timestamp=1504690473368'.format(data[0]))
        insert_db([data[2],r.text])

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
    dict_writer.writerow({"name":mdata[0],'name_en':mdata[1],'country':mdata[2],'suggestion':mdata[3]})
    print '-over--->',mdata[0]

def encode_unicode(str):
    return str.replace('\u00', '\\x').decode('string-escape').encode('utf8')

def process_text(str):
    lis = re.split('[ \n\t ]',str)
    liss = filter(lambda x: x!='', lis)
    return liss

#['countryName', 'countryUrl', 'cityName', 'cityUrl']
def get_suggestions_elong(url):
    city_list = get_datas_from_file('booking_city_list.csv')
    all_ids =[]

    def task(city):
        print city
        ul = url.format(city)
        suggestion = request_suggestion(ul)['data']
        index = -1
        print '--process-->',city
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

def get_suggestions_hotels(url):
    city_list = get_datas_from_file('booking_city_list.csv') 
    all_ids = []

    def task(city):
        ul = url.format(city[-2])
        try:
            suggestion = request_suggestion(ul)['sr']
        except:
            return
        for su in suggestion:
            if su['type'] == 'MULTICITY' or su['type'] == 'CITY':
                if all_ids.count(su["essId"]['sourceId']) == 0:
                    all_ids.append(su["essId"]['sourceId'])
                    data = [su["regionNames"]["shortName"],su["essId"]["sourceName"],su["hierarchyInfo"]["country"]["name"],su]
                    process_data(data)
    for city in city_list:
        g = execute_pool.apply_async(task, args=(city,))
        gs.append(g) 
    gevent.joinall(gs)

def get_suggestions_ctrip(url):
    city_list = get_datas_from_file('booking_city_list.csv')
    all_ids = []
    def task(city):
        #ul = url.format(city[-2])
        ul = url.format(city)
        try:
            suggestion = request_suggestion(ul)['data']
        except Exception as e:
            print e
            return
        lis = re.findall('(\|.*?\|city\|.*?\|)',suggestion)
        for li in lis:
            ll = li.split('|')
            if all_ids.count(ll[-2]) == 0:
                all_ids.append(ll[-2])
                data = [ ll[1],' ',' ',li]
                process_data(data)
    gs = [] 
    for city in city_list:
        g = execute_pool.apply_async(task, args=(city,))
        gs.append(g)
    gevent.joinall(gs)

if __name__ == '__main__':
    csvFile = open("abc_ctrip_hotel_suggestion_list.csv", "w")
    fileheader = ["name",'name_en',"country","suggestion"]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    
    url4= 'http://ihotel.elong.com/ajax/sugInfo?datatype=Region&keyword={}'
    url2 = 'https://lookup.hotels.com/suggest/v1.3/json?locale=zh_CN&query={}'
    url1 = 'https://suggest.expedia.com/api/v4/typeahead/{}?locale=zh_CN&lob=HOTELS&format=jsonp'
    url3= 'http://hotels.ctrip.com/international/Tool/cityFilter.ashx?keyword={}'
    #get_suggestions_ctrip(url3)
    get_suggestions_elong(url4)


