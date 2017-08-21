#!//bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-08-04
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

import pymysql
from DBUtils.PooledDB import PooledDB

# db ip
'''
db psw
'''

mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)

import operator
import math
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
    return int(math.sqrt(lx * lx + ly * ly))
def get_dist_by_map(map_1, map_2):
    try:
        return getDistSimply(float(map_1.split(',')[0]), float(map_1.split(',')[1]),
                             float(map_2.split(',')[0]), float(map_2.split(',')[1]))

    except Exception, e:
       #print ('get_dist', ['map = ' + map_1 + '\t' + map_2])
        #print map_2
        return 100000000000
#ditence end

def r_map(map_info):
	try:
		ma = ''
		li = map_info.split(',')
		ma = li[1]+','+li[0]
		return ma
	except:
		return ''

def get_city_info(mid):
	db = mysql_db_pool.connection()
	cur = db.cursor()
	cur.execute('select map_info from city where id = "%s"' % mid)
	try:
		c_datas = cur.fetchall()[0]
	except:
		return ''
	db.close()
	return c_datas

def get_mid(data):
	#print "this is mid:",data[5].split(',')[2][2:-1]
	return data[5].split(',')[-2][2:-1]
def get_cmap_info(data):
	try:
		return r_map(get_city_info(get_mid(data))[0])
	except:
		return ''
def get_hmap_info(data):
	return data[7].split(',')[0][2:-1]+','+data[7].split(',')[1][2:-2]


def check_match(row):
	name_en = row[5].split(',')[0][2:-1]
	currect_name_en = row[6]
	if re.search('[ |,]'+str(name_en).lower()+'[ |,]',' '+str(currect_name_en).lower()+' ' ) and name_en != '' :
		return True
	return False
def main(sortedlist):
	with open("%s_sorted.csv"%source, "wb") as f:
		fileWriter = csv.writer(f, delimiter=',')
		fla, ans1, ans2, sid = 0 ,[] ,[] ,sortedlist[0][1]
		print 'start fix'

		for row in sortedlist:
			if check_match(row):
				row[8]='YES'
			else:
				row[8]='NO'
			if sid == row[1] :
				if row[8]=='YES':
					ans1.append(row)
				else :
					ans2.append(row)
			else:
				if len(ans1)>1:		#多个匹配，取距离最小
					min_d = 100000000000
					min_i = 0
					for i in xrange(len(ans1)):
						ans1[i][8]='NO'
						d =get_dist_by_map( get_cmap_info(ans1[i]),get_hmap_info(ans1[i]) )
						#print 'map1:',ans1[i][1],ans1[i][2],get_cmap_info(ans1[i]),get_hmap_info(ans1[i])
						print 'd1',d
						if d < min_d:
							min_i = i
							min_d = d
					for i in xrange(len(ans1)):
						if i==min_i:
							ans1[i][8]='YES'
						ans2.append(ans1[i])
				else:
					if len(ans1)==1:   #有一个匹配
						ans2.append(ans1[0])
					else:		#没有匹配
						min_d = 100000000000
						min_i = 0
						d = 0
						for i in xrange(len(ans2)):
							d = get_dist_by_map( get_cmap_info(ans2[i]),get_hmap_info(ans2[i]) )
							#if ans2[i][1]=='497095':
							#	print 'map2:',ans2[i][1],ans2[i][2],get_cmap_info(ans2[i]),get_hmap_info(ans2[i])
							#	print 'd2',d
							print 'd2',d
							if d < min_d:
								min_i = i
								min_d = d
						if min_d==100000000000:
							pass
						else:
							for i in xrange(len(ans2)):
								if i == min_i :
									ans2[i][8]='PASS'
									break
				ans1=[]
				sid = row[1]
				for r in ans2:
					if fla==0:
						fla=1
						fileWriter.writerow(fileheader)
					fileWriter.writerow(r)
				ans2=[]
				try:
					if row[8]=='YES':
						ans1.append(row)
					else :
						ans2.append(row)
				except:
					print row
		if fla==0:
			print 'a 0'
			fileWriter.writerow(fileheader)

		if len(ans1)==0 and len(ans2)>1:
			min_d = 100000000000
			min_i = 0
			d = 0
			for i in xrange(len(ans2)):
				d = get_dist_by_map( get_cmap_info(ans2[i]),get_hmap_info(ans2[i]) )
				if d < min_d:
					min_i = i
					min_d = d
			if min_d==100000000000:
				pass
			else:
				for i in xrange(len(ans2)):
					if i == min_i :
						ans2[i][8]='PASS'
						break

		for r in ans1:
			print 'b 0'
			fileWriter.writerow(r)
		for r in ans2:
			print 'c 0'
			fileWriter.writerow(r)
	    		#
	f.close()


if __name__=='__main__':
	fileheader = ["source", "sid", "uid", "name", "name_en", "城市信息", "API_adress", "坐标", "是否匹配", "源URL"]
	source = sys.argv[1]

	data = csv.reader(open('%s.csv'% source),delimiter=',')
	data.next()
	sortedlist = sorted(data, key = lambda x: (x[0], int(x[1])))

	main(sortedlist)
	
	print 'finished:{}'.format(source)

