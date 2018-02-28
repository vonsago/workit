#!//bin/python
# -*- coding: UTF-8 -*-

'''
@date : 2017-08-18
@author : vassago
@update : 2018-01-29
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

def test_qyer_result(data):
    sight, food, shopping, activity = {}, {}, {}, {}
    for da in data:
        if da[-2] =='sight':
            sight[da[1]]=da
        if da[-2] =='food':
            food[da[1]]=da
        if da[-2] =='shopping':
            shopping[da[1]]=da
        if da[-2] =='activity':
            activity[da[1]]=da
    a,b,c,d = len(sight.keys()),len(food.keys()),len(shopping.keys()),len(activity.keys())
    print '---',len(sight.keys()),'---',len(food.keys()),'---',len(shopping.keys()),'---',len(activity.keys()),'--total--',a+b+c+d

def test_qyer_from_mongodb(host = None, port = 27017):
    client = pymongo.MongoClient(host)
    collections = client['data_result']['qyer_list_bak']
    datas = "48c9a9555f94acfb1a179d159a9a8af5|5710ef67b48a169c0d21697cbb3ea67b|a8c57267c0a36e4f24e79c846b08a2e0|4aa3e3fc6ea92a0f4f653dfbc51647fe|daa754f730e09a643b4cde57fe0616e0"
    for data in datas.split('|'):
        print data
        for res in collections.find({"task_id":data}).sort([('used_times', 1)]):
            print res['total_num'],res['used_times']
            if res != None:
                test_qyer_result(res['result'])


def get_data_from_mongodb(host = None, port = 27017):
    client = pymongo.MongoClient(host)
    collections = client['SuggestName']['CtripCitySuggestion']
    sql = """INSERT INTO ota_location(
    source,sid_md5,sid,suggest_type, suggest, city_id, country_id, s_city, s_region, s_country, s_extra,label_batch
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    se = set()
    for data in collections.find():
        datas = json.loads(data['suggest'])['Data']
        if datas == None:
            continue
        for d in datas:
            db_data = parse_data_to_db(d)
            md5 = db_data[1]
            l = len(se)
            se.add(md5)

            if l< len(se):
                insert_db(db_data,sql)
    print len(se)

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

def ctripPoi_detail2mysql():
    sql = """INSERT INTO ctrip_poi_detail(
    poi_id, source, poi_type, name, name_en, city_id, map_info, address, telephone, introduction, grade, beentocount, plantocount, commentcount, image_url, image_num, 
    grade_detail, comment_category_detail, highlight, visit_time, website, open_time, ticket, price, tag, city_info, url, traffic, tips
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    poi_type_dic = {'sight':1,'shop':2,'food':3,'shopping':2}
    collections = client['data_result']['ctrip_poi_detail']
    num = 0
    for coo in collections.find({'collections':'Task_Queue_poi_detail_TaskName_detail_total_ctripPoi_20180211q'}).sort("_id",pymongo.DESCENDING):
        if coo['result']!=[]:
            co = coo['result']
            try:
                poi_id = coo['poi_id']
            except:
                num+=1
                continue
            poi_type = poi_type_dic[coo['tag']]
            url = coo['url']
            s = co[12].split(',')
            co[12] = s[1]+','+s[0]

            city_id = ''
            if citydic.has_key(co[4].encode('utf-8')):
                city_id = citydic[co[4].encode('utf-8')]
                print 'city_id',city_id
            if co[6] == '':
                co[6] = 0
            data = [poi_id,'ctripPoi', poi_type, co[0], co[1], city_id,co[12], co[13], co[17], co[11], co[5],0,0, 
                    int(co[6]), co[7],co[8],json.dumps(co[9]), json.dumps(co[10]),
                    co[15], co[16], co[18], co[19], co[20], co[21], co[14], co[4] ,url, co[-2].encode('utf-8'), co[-1]]
            insert_db(data,sql)
    print num

def mmp():
    import pymongo
    import pymongo.errors
    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    db = client['SuggestName']
    all = []
    for ce in db.CtripPoiSDK.find({}):
        try:
            s = ce['suggest']['List']
        except:
            continue
        for sug in ce['suggest']['List']:
            args = {
                'name': sug['Name'],
                'dest_name': sug['DestName'],
                'keyword': sug['Url'].split('.')[0].split('/')[-1]
            }
            all.append(args)
    print(len(all))
    with InsertTask(worker='proj.total_tasks.normal_city_task', queue='supplement_field', routine_key='supplement_field',
                    task_name='Poictrip_city_20180227a', source='ctrippoidetail', _type='CityInfo',
                    priority=3, task_type=TaskType.NORMAL) as it:
        for a in all:
            it.insert_task(a)

def match_ctripPoi_city():
    import pymongo
    import json

    client = pymongo.MongoClient('mongodb://root:miaoji1109-=@10.19.2.103:27017/')
    collections = client['SuggestName']['CtripPoiSDK_detail']

    d = MiojiSimilarCityDict()

    for co in collections.find({}):
        try:
            mapdic = json.loads(co['map_info'])
            map = str(mapdic['lng'])+','+str(mapdic['lat'])
            city_id = d.get_mioji_city_id((co['dest_name'],co['name']), map)[0].cid
            client.SuggestName.CtripPoiSDK_Mioji.save({
                'city_id':city_id,
                'name':co['name'],
                'dest_name':co['dest_name'],
                'task':co['keyword'],
                'map_info':co['map_info']
            })
        except Exception as e:
            print(e)
            print(co['name'])
            pass
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
