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
'''
#db 
psw
'''

mysql_db_admin = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip, port=3306,
                         user=base_user, passwd=base_pwd, db=base_db, charset='utf8', use_unicode=False, blocking=True)

mysql_db_reader = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=100, host=base_ip1, port=3306,                                                                                                                                
                        user=base_user1, passwd=base_pwd1, db=base_db1, charset='utf8', use_unicode=False, blocking=True)


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


def process_data(mdata,flag):
    db=mysql_db_reader.connection()
    cursor = db.cursor()
    cursor.execute('select id ,name_en,tri_code,region,map_info from city where name ="{}"'.format(mdata[0]))
    try:
        data = cursor.fetchall()[0]

        if flag:
            dict_writer.writerow({"name":mdata[0],"id":data[0],"name_en":data[1],"tri_code":data[2],"region":data[3],"map_info":data[4] ,"country":mdata[1],"是否匹配":'YES'})
        else:
            dict_writer.writerow({"name":mdata[0],"id":data[0],"name_en":data[1],"tri_code":data[2],"region":data[3],"map_info":data[4] ,"country":mdata[1],"是否匹配":'NO'})
    except :
        dict_writer.writerow({"name":mdata[0],"id":'None',"country":mdata[1],"是否匹配":'NO'})

if __name__ == '__main__':
    '''
    csvFile = open("test.csv", "w")
    fileheader = ["name","country","id","name_en" ]
    dict_writer = csv.DictWriter(csvFile, fileheader)
    dict_writer.writerow(dict(zip(fileheader, fileheader)))
    match_city_with_db('hilton_city_list0.csv')
    '''


    




