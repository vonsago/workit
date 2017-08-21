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
import csv
import gevent
from gevent import pool

import pymysql
from DBUtils.PooledDB import PooledDB

# db ip
'''
db psw
'''

mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=50, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)


mem_dic={}
URL = 'http://maps.google.cn/maps/api/geocode/json?latlng='
execute_pool = pool.Pool(1000)

def get_PROXY():
	R = requests.get('http://10.10.239.46:8087/proxy?user=crawler&passwd=spidermiaoji2014&source=google')
	p = R.content
	PROXY = {
		'http': 'socks5://'+p,
	  	'https': 'socks5://'+p
	}
	return PROXY

def get_repeat_sid(source):
	db = mysql_db_pool.connection()
	cursor = db.cursor()
	cursor.execute("select sid from hotel_unid where source='%s' group by sid having count(sid)>1" % source)
	sid_lis = cursor.fetchall()
	db.close()
	return sid_lis

def get_map_data(x,y):
	i = 0
	while i<3:
		try:
			r = requests.get(URL + x + ',' + y, proxies=get_PROXY(), timeout=(5, 10))
			map_data = json.loads(r.content)
			return json.dumps(map_data['results'][0]['formatted_address'], ensure_ascii=False)
		except:
			print 'get error'
			i+=1
	return None

def get_sid_hotels(source,sid):
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select mid , map_info,uid,name,name_en,hotel_url '
                'from hotel_unid where source = "%s" and sid = "%s"' % (source, sid))

    hotel_datas_list = cur.fetchall()
    db.close()

    return hotel_datas_list

def get_city_info(mid):
    db = mysql_db_pool.connection()
    cur = db.cursor()
    cur.execute('select name_en,name,id,tri_code from city where id = "%s"' % mid)
    c_datas = cur.fetchall()[0]
    db.close()
    return c_datas

def match_city(source, sid):
    print 'start {0}'.format(sid)

    hotel_datas_list = get_sid_hotels(source, sid)
    map_info = hotel_datas_list[0][1].split(',')
    x, y = map_info[1], map_info[0]
    currect_name_en = get_map_data(x, y)


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
        if currect_name_en.find(name_en) != -1:
            dict_writer.writerow(
                {"source": source, "sid": sid, "uid": uid, "name": name, "name_en": h_name_en, "城市信息": jcity_info,
                 "API_adress": currect_name_en, "坐标": jmap_info_lis, "是否匹配": 'YES', "源URL": hotel_url})
        else:
            dict_writer.writerow(
                {"source": source, "sid": sid, "uid": uid, "name": name, "name_en": h_name_en, "城市信息": jcity_info,
                 "API_adress": currect_name_en, "坐标": jmap_info_lis, "是否匹配": 'NO', "源URL": hotel_url})

if __name__ == '__main__':
	s = time.time()
	source = 'accor'

	sid_lis = get_repeat_sid(source)
	csvFile = open("%s.csv" % source, "w")
	fileheader = ["source", "sid", "uid", "name", "name_en", "城市信息", "API_adress", "坐标", "是否匹配", "源URL"]
	dict_writer = csv.DictWriter(csvFile, fileheader)
	dict_writer.writerow(dict(zip(fileheader, fileheader)))

	gs = []
	print len(sid_lis)
	for sid in sid_lis:
		sid = sid[0]
		g = execute_pool.apply_async(match_city, args=(source, sid))
		gs.append(g)
	gevent.joinall(gs)

	csvFile.close()
	print time.time() - s
