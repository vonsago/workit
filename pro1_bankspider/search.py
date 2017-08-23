# -*-encoding: utf-8 -*-  

import os
import sys
import string
import redis
import MySQLdb  

reload(sys)
sys.setdefaultencoding('utf8')    #匹配城市对时候需要

sys.path.append('.. /common')                                                                                                                          
sys.path.append('../protoc_lib')

from match_key import Match_Key
from libBoostFlightTicketInfo import Flight_Ticket_Info

#ip and password
'''
psw mag
'''
#db_ip..
'''
db msg
'''

#命令行输入要查找的数据

city2 = ''
dept_code = ''
dest_code = ''
city_id=''
corp=''
airport_id = ''
corp_id = ''
iflight_key_list=[]
result = []

#找到航空公司对应的id  --OK
def get_corp_id():
	cursor = db.cursor()
	cursor.execute("select * from normalize_flight_corp")
	rows = cursor.fetchall()

	for row in rows:
		if(corp == row[1].encode("utf-8")):
			corp_id = row[2]
			break

#把城市对 对应为 id  --OK
def get_city_id():
	cursor = db.cursor()
	cursor.execute("select * from city")
	rows = cursor.fetchall()

	fla = 0
	for row in rows :
		if(row[1]==dept_code.encode('utf-8')):
			dept_code=row[0]
			fla+=1
		if(row[1]==dest_code.encode('utf-8')):
			dest_code=row[0]
			fla+=1
		if(fla==2):
			city_id = dept_code+"_"+dest_code
			break

#  --OK
def get_iflight_all_key():
	try:
		r = redis.Redis(host=iflight_redis_ip, port=port, password=password)
	except Exception ,e:
		print 'iflight_redis error'

	tmp_list = r.execute_command('SCAN',0,'count',1000)
	while tmp_list[0]!='0':
		it=int(tmp_list[0])
		for k in tmp_list[1]:
			iflight_key_list.append(k)
		tmp_list=[]
		tmp_list = r.execute_command('SCAN',it,'count',1000)

def from_protocol_get_corp_id(pro_datas):
	tmp_pb = Flight_Ticket_Info()
	tmp_pb.ParseFromString(pro_datas)
	return tmp_pb.norm_corp_code_vec(0)


def get_result():

	get_iflight_all_key()
	#连接md5_redis
	try:
		md5_dats_redis = redis.Redis(host=md5_redis_ip, port=port, password=password)
	except Exception ,e:
		print 'md5_redis error'

	for ikey in range( iflight_key_list ):
		#遍历所有ikey，找到其中与输入城市对匹配的key
		if( ikey.find(city_id) != -1):
			flight_key_list=iflight_key_redis.smembers(ikey)

			#对匹配的key中的md5进行遍历，筛选符合所给航空公司的数据
			for k in range( flight_key_list ):

				price_md5 = flight_key.get(k)
				md5 = price_md5[5]

				protocol_datas = md5_dats_redis.get(md5)    #从md5redis获取对应的value  
				norm_corp_id = from_protocol_get_corp_id(protocol_datas)  #将protocol数据解析得到航空公司Id

				#筛选出符合的航空公司id，并将价格等信息提取出
				if(norm_corp_id == corp_id):
					result.append(price_md5[0 :len(price_md5)-18])


if __name__ == '__main__':
	#命令行输入要查找的数据
	city2 = str(sys.argv[1])
	index_=city2.find('_')
	dept_code = city2[0: index_]
	dest_code = city2[index_+1 :len(city2)]
	
	corp=str(sys.argv[2])

	#连接数据库
	try:
		db = MySQLdb.connect(host = base_ip,user = base_user,charset = 'utf8',passwd = base_pwd,db = base_db)
	except Exception , e:
		print 'db error'

	get_corp_id()
	get_city_id()
	db.close()

	get_result()
	print result

