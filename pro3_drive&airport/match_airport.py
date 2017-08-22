#!//bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-08-22
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

import trollius as asyncio
from trollius import From
#db
'''
db psw
'''
mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)

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

def get_airport(iata):
    db = mysql_db_pool.connection()
    cur = db.cursor()
    try:
        cur.execute('select iata_code,name_en,map_info ,country from airport where iata_code="{0}"'.format(iata))
        c_datas = cur.fetchall()
        db.close()
        return c_datas[0]
    except:
        return None
def change_map(map_info):
    try:
        li = map_info.split(',')
        a = li[1]+','+li[0]
        return a
    except:
        return ''

def get_data_from_txt():
    f = open('airport.txt','r')
    data = f.readlines()[0][33:-3]
    data = data.split('},{')
    return data

def process_match():
    datas = get_data_from_txt()
    #"name":"A Coruna Airport","iata":"LCG","icao":"LECO","lat":43.302059,"lon":-8.37725,"country":"Spain","alt":326
    print "total:",len(datas)
    null_sum=0
    normal_sum =0
    error_sum =0
    for data in datas:
        data ='{'+ data+'}' 
        dic=eval(data)

        air_data = get_airport(dic['iata'])
        if air_data!=None:
            m1 =str(dic['lat'])+','+str(dic['lon'])
            m2 = change_map(air_data[2])
            if m2 == '':
                null_sum += 1    
                m2="'null','null'"
                dist = 'null'
            else:
                dist = get_dist_by_map(m1,m2)
            m = m2.split(',')
            if dist<=100 and dist!='null':
                normal_sum+=1
            #('LCG', 'A Coru\xc3\xb1a Airport', '-8.381994,43.302367', '\xe8\xa5\xbf\xe7\x8f\xad\xe7\x89\x99')
            dict_writer.writerow({'name':dic['name'],'iata':dic['iata'],'icao':dic['icao'],'lat':dic['lat'],'lon':dic['lon'],'country':dic['country'],'alt':dic['alt'],'分割线':'|','m_name':air_data[1],'m_iata':air_data[0],'m_lat':m[0],'m_lon':m[1],'m_country':air_data[3],'dist':dist})
        else:
            error_sum += 1
            dict_writer.writerow({'name':dic['name'],'iata':dic['iata'],'icao':dic['icao'],'lat':dic['lat'],'lon':dic['lon'],'country':dic['country'],'alt':dic['alt'],'分割线':'|'})

    print 'null_map_sum :',null_sum
    print 'dist is equal sum :',normal_sum
    print 'no this airport in db :',error_sum


if __name__ == '__main__':
    csvFile = open("airport.csv", "w")
    #{"name":"A Coruna Airport","iata":"LCG","icao":"LECO","lat":43.302059,"lon":-8.37725,"country":"Spain","alt":326}
    fileheader = ['name','iata','icao','lat','lon','country','alt','分割线','m_name','m_iata','m_name_en','m_lat','m_lon','m_country','dist']
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    #dict_writer.writerow() 
    process_match()


