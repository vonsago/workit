#!//bin/python
# -*- coding: UTF-8 -*-

'''
@date : 2017-08-18
@author : vassago
@update : 2018-03-27
@email : f811194414@gmail.com
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

execute_pool = pool.Pool(2000)
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


Google_URL ='http://maps.google.cn/maps/api/geocode/json?address={}'
final = {}
def re_request(url,j):
    print '-----proces---- ',url
    i =0
    while i <5:
        try:
            r = requests.get(url, proxies=get_PROXY(), timeout=(5, 10))
            api_data = r.content
            #if json.loads(api_data)['status']!='OK':
            #raise 'not ok'
            final[j] = json.loads(api_data)['results'][0]['geometry']['location']
            print 'OK'
            return
        except Exception as e:
            print e,'--and retry---'
            i+=1
            if i == 5:
                print url,'----fu--'

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

def get_datas_from_file(fname, item = False, limit = 100000):
    csv.field_size_limit(limit)
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
                final.append(data.strip())
            return final
    def get_csv_item(fname):
        with open(fname) as f:
            f_csv = csv.reader(f)
            _ = next(f_csv)
            print _
            for row in f_csv:
                yield row
    def get_forml(fname):
        with open(fname) as f:
            datas = f.readlines()
            for data in datas:
                yield data.strip()

    if fname.find('.')>0:
        if fname.split('.')[-1]=='csv':
            if item:
                get_csv_item(fname)
            else:
                return get_data_from_csv(fname)
        else:
            return get_data_from_forml(fname)

def get_data_from_db(sql):
    db=mysql_db_admin.connection()
    cursor = db.cursor()
    cursor.execute(sql)
    db.close()
    return cursor.fetchall()

def process_datas_to_csv(datas,file_name,header):
    csvFile = open("{}.csv".format(file_name), "w")
    fileheader = header
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))

    for data in datas:
        dict_writer.writerow(dic(zip(header,data)))
    #csvFile.close()
    print '-over--->',data[0]

def insert_db(data):
    db = mysql_db_admin.connection()
    cur = db.cursor()
    sql = """INSERT INTO hotel_suggestions_city(
    city_id, source, suggestions, select_index,annotation
    )
    VALUES (%s,%s,%s,%s,%s)"""
    try:
        cur.execute(sql,(data))
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
#----google map info
def retry(times=3, raise_exc=True):
    def wrapper(func):
        @functools.wraps(func)
        def f(*args, **kwargs):
            _exc = None
            for i in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    _exc = exc
                    logger.exception(msg="[retry exception][func: {}][count: {}]".format(func.__name__, i),
                                     exc_info=exc)
            if _exc and raise_exc:
                raise _exc

        return f

    return wrapper
@retry(times=4, raise_exc=False)
def google_get_map_info(address):
    with proj.my_lib.Common.Browser.MySession(need_cache=True) as session:
        page = session.get('https://maps.googleapis.com/maps/api/geocode/json?address=' + quote(address))
        results = json.loads(page.text).get(
            'results', [])
        if len(results) == 0:
            raise Exception("length 0")

        map_info = results[0].get('geometry', {}).get('location', {})

        try:
            longitude = float(map_info.get('lng', None))
            latitude = float(map_info.get('lat', None))
        except Exception as e:
            raise e
        return str(Coordinate(longitude, latitude))

def process(args):
    '''
    work
    '''
    return result


def get_data_from_mongodb(host = None):
    #from pymong.cursor import CursorType
    #find(cursor_type=CursorType.EXHAUST)
    #
    client = pymongo.MongoClient(host)
    collections = client['SuggestName']['CtripCitySuggestion']
    for data in collections.find({},[],):
        pass

def parse_data_to_db(d):

    md5 = calc_md5(d['Poid'])
    s_city = d['Name']
    countrys = d['ParentName'].split(',')
    s_country = countrys[-1]
    s_region = countrys[0]
    if s_region == s_country :
        s_region ="NULL"
    s_extra = "NULL"
    country_id = 'NULL'
    city_id = 'NULL'
    return ['ctrip_grouptravel',md5,d['Poid'], 2, json.dumps(d), city_id, country_id, s_city, s_region,s_country, s_extra, '2018-01-25a']

def calc_md5(before = None):
    m = hashlib.md5()
    m.update(str(before))
    return m.hexdigest()


def report_country():
    countryss = get_data_from_db('SELECT name FROM country')
    countrys = []
    for coun in countryss:
        countrys.append(coun[0])

    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')


    csvFile = open("ctripPoi_country.csv", "w")
    fileheader = ['country','city_num','mioji']
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))

    from collections import defaultdict
    coun_city = defaultdict(set)
    collections = client.SuggestName.CtripPoiSDK_detail
    for co in collections.find({}):
        if co['map_info'] == '':
            continue
        coun = co['dest'].split('|')[0]
        if coun == '':
            continue
        coun_city[coun].add(co['name'])
    count = set()
    for cc in coun_city.items():
        city = len(cc[1])
        if cc[0].encode('utf-8') in countrys:
            count.add(cc[0].encode('utf-8'))
            dict_writer.writerow(dict(zip(fileheader, [cc[0], city,'yes'])))
        else:
            dict_writer.writerow(dict(zip(fileheader, [cc[0], city,'no'])))

    print len(countrys)
    count = list(count)
    for cc in countrys:
        if cc not in count:
            dict_writer.writerow(dict(zip(fileheader, [cc, 0,'yes'])))
def fix_html(data):
        aa = re.findall('&#\d+;',data)
        xx = [html.fromstring(i).text_content() for i in aa]
        yy = re.sub('&#\d+;','{}',data)
        yy = yy.split('{}')
        res = yy[0]
        for i in range(len(xx)):
            res += xx[i]+yy[i+1]
        return res


def restart_task(data):
    client = pymongo.MongoClient('')
    db = client.MongoTask
    db[data].update_many({'finished':0,'used_times':7}, {
        '$set': {
        'finished': 0,
        'running': 0,
        'used_times': 0
        }
        });

def factory():
    return getattr(sys.modules[__name__], '')

def data2csv():
    pass

def req(url):
    p = requests.get("").content
    proxy = {'http': 'socks5://'+p,'https': 'socks5://'+p}
    datas = requests.get(url,proxies=proxy).content
    db.data_result.ctripPoi_image_url.insert_one({

        })
def req_muli(task):
    gs = []
    for i in range(task):
        g = execute_pool.apply_async(req,args=(url,))
        gs.append(g)
    gevent.joinall(gs)

if __name__ == '__main__':
    '''
    csvFile = open("test.csv", "w")
    import codecs
    csvFile.write(codecs.BOM_UTF8)
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


    #-----
    citys = get_data_from_db('SELECT id,name FROM city')
    print len(citys)
    citydic = {}
    for city in citys:
        citydic[city[-1]]=city[0]
    countrys = get_data_from_db('SELECT mid,name,name_en FROM country')

    print len(countrys)
    countrydic = {}
    for country in countrys:
        countrydic[country[1]]=country[0]
        countrydic[country[2]]=country[0]
    #-----
