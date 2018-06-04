#!/usr/bin/python
# -*- coding: UTF-8 -*-

'''
date : 2017-07-27
@author: fengyufei
'''
import re
import sys
import requests
import json
import MySQLdb
reload(sys) 
sys.setdefaultencoding('utf-8')

DATE_F = '%Y-%m-%d'
# db ip
#db=MySQLdb.connect(host = base_ip,user = base_user,charset = 'utf8',passwd = base_pwd,db = base_db)
'''
db msg
'''

def cbankspider():
    try:
        f = open ('result','w')
    except Exception,e:
        print 'open file error'

    fla = 0
    all_current_list = []

    URL = 'url'

    for index in range(10):
        if(index>0):
            URL = 'http://www.boc.cn/sourcedb/whpj/index_' + str(index) + '.html'
        try:
            r = requests.get(URL)
        except Exception,e:
            print 'requests error'
        HTML = r.text.encode('raw-unicode-escape')
        data = HTML[HTML.find('发布时间'):HTML.find('--发布-end')]

        data_list = data.split('</tr>')
        
        l = len(data_list)
        for i in range(l):
            if i==0 or i==l-1 :
                continue

            result_lis = re.findall('<td>.*</td>',data_list[i])
            name = result_lis[0][4:-5]
            sell1 = result_lis[3][4:-5]
            sell2 = result_lis[4][4:-5]
	    date = result_lis[6][4:-5]+' '+result_lis[7][4:-5]
            di = {'name': name, 'data': [sell1, sell2], 'date': date,'id' :' ' }

            if(sell1 == ''):
                di['data'].append(str(float(sell2)/100))
            else :
                di['data'].append(str(float(sell1)/100))

            all_current_list.append(result_lis[0][4:-5])
             
            f.write(json.dumps(di,ensure_ascii=False))
            f.write('\n')

    f.close()
    return all_current_list

def update_db():
    try:
	db = MySQLdb.connect(host = base_ip,user = base_user,charset = 'utf8',passwd = base_pwd,db = base_db)
    except Exception ,e:
        print 'db connect error'
    cursor = db.cursor()
    cursor.execute('select * from exchange')
    rows= cursor.fetchall()
    f = open('result','r')
    datas = f.readlines()
    for data in datas:
	data = json.loads(data)
	    #print row[1],' ',row[5]
	for row in rows:
	    if row[1]==data['name']:
		data['id']=str(row[0])
		cursor.execute("update exchange set rate='%s',update_time = '%s' where id ='%s'"%(data['data'][2],data['date'],row[0]))
		db.commit()
		break
    db.close()
if __name__ == '__main__':
    lis = cbankspider()
    
    update_db()
