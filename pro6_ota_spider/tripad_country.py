#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@author: vassago
@date: 2017-09-21
'''

import requests
from lxml import html as HTML
import math
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


headers1 = {'Accept':'text/javascript, text/html, application/xml, text/xml, */*',
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6',
            'Connection':'keep-alive',
            'Cookies':'TAReturnTo=%1%%2FTourism-g294232-Japan-Vacations.html;',
            'Referer':'https://www.tripadvisor.cn/Tourism-g294196-South_Korea-Vacations.html',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.91 Safari/537.36',
            'X-Puid':'WcMln8CoAWgAArPwdskAAABM',
            'X-Requested-With':'XMLHttpRequest',
            }

headers2 = {'Accept':'text/javascript, text/html, application/xml, text/xml, */*',
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6',
            'Connection':'keep-alive',
            #'Cookies':'TAUD=LA-1505965483111-1*RDD-1-2017_09_20*LG-90580-2.1.F.*LD-90581-.....; Domain=.tripadvisor.cn; Expires=Thu, 05-Oct-2017 03:46:14 GMT; Path=/;TASession=%1%V2ID.338CB0A1486EB0F625BC0FB6983E0102*SQ.8*LP.%2FLvyou*LS.Tourism*GR.93*TCPAR.51*TBR.51*EXEX.4*ABTR.50*PHTB.44*FS.61*CPU.45*HS.recommended*ES.popularity*AS.popularity*DS.5*SAS.popularity*FPS.oldFirst*FA.1*DF.0*TRA.true*LD.294232; Domain=.tripadvisor.cn; Path=/',
            'Referer':'https://www.tripadvisor.cn/Tourism-g294196-South_Korea-Vacations.html',
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.91 Safari/537.36',
            'X-Puid':'WcMln8CoAWgAArPwdskAAABM',
            'X-Requested-With':'XMLHttpRequest',
}


def request(url, headers=None):
	r = requests.get(url, headers = headers)
	html_content =  r.text.encode('utf-8')
	root = HTML.fromstring(html_content)
	root_list = [root]
	if headers != None:
		url_code = re.findall('-g(.*?)-',url)[0]
		if len(root.find_class('morePopularCities ui_button primary chevron') ) != 0:
			headers2['Cookies'] = r.headers['Set-Cookie']
			page = re.findall("tourism\.popularCitiesMaxPage\', \'(.*?)\'",html_content)[0]
			for i in range(1,int(page)+1):
				r2 = requests.get('https://www.tripadvisor.cn/TourismChildrenAjax?geo={}&offset={}&desktop=true'.format(url_code,str(i)),headers = headers2)
				headers2['Cookies'] = r2.headers['Set-Cookie']
				html_content = r2.text.encode('utf-8')
				root_list.append(html_content)

	return root_list

def get_country():
    root = request('https://www.tripadvisor.cn/Lvyou')[0]
    country_list = root.xpath('//div[@id="tab-body-wrapper-ipad"]/div')
    result = []
    for li in country_list:
        country = li.xpath('ul')
        for coun in country:
            co = coun.xpath('li')
            for c in co:
                img_url = c.xpath('a/@href')
                url = img_url[0]
                name = c.text_content().replace('\n','').replace(' ','').replace('\r','')
                result.append([name,'https://www.tripadvisor.cn'+url])
                print name,url
    return result

def get_city(root_list):
	result = []
	for root in root_list:
		try:
			city_list = root.xpath('//div[@class="popularCities"]/a')
			for city in city_list:
				img_url = 'https://www.tripadvisor.cn'+ city.xpath('@href')[-1]
				city_name = city.xpath('div[@class="cityName"]/div/span[@class="name"]')[0].text_content()
				result.append([city_name,img_url])
		except:
			name_list = re.findall('class=\"name\">(.*?)<',root)
			url_list = re.findall('href=\"(.*?)\"',root)
			if name_list[-1] != result[-1][0]:
				for i in range(len(name_list)):
					result.append( [name_list[i], 'https://www.tripadvisor.cn'+url_list[i]])
	return result

def return_file(country,final):
	with open('daodao_country_list','a') as f:
		f.write('<---country--->'+country[0]+','+country[1]+'\n')
		for i in final:
			f.write(i[0]+','+i[1]+'\n')


def process_country(country_list):
	final = []
	i = 1
	for country in country_list:

		print 'prcess--->',country[0],country[-1]
		headers1['Cookies'] = 'TAReturnTo='+country[-1][26:]
		result = get_city(request(country[-1],headers1))

		for i in result:
			print i[0],i[-1]
		print len(result)
		return_file(country,result)
		print 'prcess over--->',country[0]
		

if __name__ == '__main__':
	process_country(get_country())
	