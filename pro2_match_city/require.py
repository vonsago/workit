#!//bin/python
# -*- coding: UTF-8 -*-

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

import math


source = sys.argv[1]

csvFile = open("%s_new.csv" % source, "w")
fileheader = ["source", "sid", "uid", "name", "name_en", "城市信息", "API_adress", "坐标", "是否匹配", "源URL"]
dict_writer = csv.DictWriter(csvFile, fileheader)
dict_writer.writerow(dict(zip(fileheader, fileheader)))


cf = open('%s_sorted.csv'%source,'r')

read = csv.reader(cf)
read.next()

for r in read:
    name_en = r[5].split(',')[0][2:-1]
    currect_name_en = r[6]
    if r[8]=='PASS':
    	dict_writer.writerow({"source": r[0], "sid": r[1], "uid": r[2], "name": r[3], "name_en": r[4], "城市信息": r[5],"API_adress": r[6], "坐标": r[7], "是否匹配": 'PASS', "源URL": r[9]})
    elif re.search('[ |,]'+str(name_en).lower()+'[ |,]',' '+str(currect_name_en).lower()+' ' ) and name_en != '' :
        dict_writer.writerow({"source": r[0], "sid": r[1], "uid": r[2], "name": r[3], "name_en": r[4], "城市信息": r[5],"API_adress": r[6], "坐标": r[7], "是否匹配": 'YES', "源URL": r[9]})
    else:
        dict_writer.writerow({"source": r[0], "sid": r[1], "uid": r[2], "name": r[3], "name_en": r[4], "城市信息": r[5],"API_adress": r[6], "坐标": r[7], "是否匹配": 'NO', "源URL": r[9]})


