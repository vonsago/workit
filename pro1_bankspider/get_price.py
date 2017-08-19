# -*-encoding: utf-8 -*-

import os
import sys
import string
import redis
import MySQLdb  
reload(sys)  

sys.path.append('../common')
sys.path.append('../protoc_lib')
from match_key import Match_Key
from libBoostFlightTicketInfo import Flight_Ticket_Info
sys.setdefaultencoding('utf8')
#ip and password
iflight_redis_ip="10.19.51.101"
md5_redis_ip="10.10.240.17"
password = 'MiojiPikaOrz'
port=9221

#db_ip..
base_ip = "10.10.69.170"
base_user = "reader"
base_pwd = "miaoji1109"
base_db = "base_data"

result_flag=0

def get_city_code(dept,dest):
	try:
		db = MySQLdb.connect(host = base_ip,user = base_user,charset = 'utf8',passwd = base_pwd,db = base_db)
	except Exception ,e:
		print 'db error'
	
	cursor = db.cursor()
	cursor.execute("select * from city")
	rows = cursor.fetchall()
	dept_=''
	dest_=''
	fla1=0
	fla2=0
	for row in rows:
		if(row[1]==dept.encode('utf-8')):
			dept_=row[3]
			fla1+=1
		if(row[1]==dest.encode('utf-8')):
			dest_=row[3]
			fla2+=1
		if(fla1==1 and fla2==1):
			city_id = dept_+"_"+dest_
			return city_id
	
def get_corp_id(corp):
	try:
		db=MySQLdb.connect(host = base_ip,user = base_user,charset = 'utf8',passwd = base_pwd,db = base_db)
	except Exception,e:
		print 'db error'
	cursor = db.cursor()
	cursor.execute("select * from normalize_flight_corp")
	rows = cursor.fetchall()
	for row in rows:
		if(corp == row[1].encode("utf-8")):
			return row[0]

def get_all_iflight_key():
	r = redis.Redis(host=iflight_redis_ip, port=port, password=password)
	fli = open('all_iflight_key','w') 
	
	tmp_list = r.execute_command('SCAN',0,'count',2000)
	while tmp_list[0] != '0':
		l = int(tmp_list[0])
		for k in tmp_list[1]:
			fli.write(k + '\n')
			tmp_list = []
		tmp_list = r.execute_command('SCAN',l,'count',2000)
	fli.close()


def from_protocol_get_corp_id(md5):
	try:
		r = redis.Redis(host=md5_redis_ip, port=port, password=password)
	except Exception ,e:
		print 'md5_redis error'
	nor_cor_cod_vec=[]
	pro_datas =str(r.get(md5))
	tmp_pb = Flight_Ticket_Info()
	tmp_pb.ParseFromString(pro_datas)

	for i in range(tmp_pb.norm_corp_code_vec_size()):
		nor_cor_cod_vec.append(tmp_pb.norm_corp_code_vec(i))
	return nor_cor_cod_vec


	
def get_result(city_id,corp_id):
	try:
		r = redis.Redis(host=iflight_redis_ip, port=port, password=password)
	except Exception ,e:
		print 'redis error'
	
	result_flag=0

	f = open('result','w')
	ori_key = open('all_iflight_key','r')

	key_lis = ori_key.readlines()
	
	match_key = 'flight_'+city_id

	for flight_key in key_lis :
		if(flight_key.find(city_id) != -1 and flight_key[0:6]=='flight'):
			price_md5_list=str(r.get(flight_key[0:flight_key.find('\n')])).split('\n')
			fla=0
			for k in price_md5_list:
				if fla==0:
					fla=1
					continue
				lis = k.split('\t')
				md5=str(lis[len(lis)-1])
				norm_corp_id = from_protocol_get_corp_id(md5)
					
				for i in norm_corp_id:
					if(i ==corp_id ):
						result_flag=1
						f.write(lis[0]+'\n')
			price_md5_list=[]
	if(result_flag==0):
		f.write('none')
	
	ori_key.close()
	f.close()
	

if __name__ == '__main__':
	dept = sys.argv[1]
	dest = sys.argv[2]
	corp = sys.argv[3]
	city_id = get_city_code(dept,dest)
	corp_id = get_corp_id(corp)
	get_all_iflight_key()
	get_result(city_id,corp_id)

