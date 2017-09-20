#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@author: vassago

@date: 2017-09-20
'''

import requests
from lxml import html as HTML
import math
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def request(url):
	r = requests.get(url)
	html_content =  r.text.encode('utf-8')
	root = HTML.fromstring(html_content)
	return root

def get_country():
	root = request('http://place.qyer.com/')
	country_list = root.xpath('//div[@class="pla_indcountrylists"]/div')
	result = []
	for li in country_list:
		lines = li.xpath('div')
		for line in lines:
			countries = line.xpath('ul/li')
			for country in countries:
				img_url = country.xpath('a/@href')
				if len(img_url) == 0:
					img_url = country.xpath('p/a/@href')
				url = img_url[0]
				name = country.text_content().replace('\n','').replace(' ','').replace('\r','')
				result.append([name,url])
	return result
#//div[@class="ui_page"]

def get_city(root):
	result = []
	city_list = root.xpath('//ul[@class="plcCitylist"]/li')
	for city in city_list:
		img_url = 'http:'+ city.xpath('h3/a/@href')[-1]
		city_name = city.xpath('h3')[0].text_content().replace('\n','').replace(' ','').replace('\r','').replace('\xc2\xa0\xc2\xa0',',')
		result.append( [city_name,img_url])
	return result

def get_max_page_num(root):
	pages = root.xpath('//div[@class="ui_page"]/a')
	if len(pages)==0:
		return 0
	nums = []
	for page in pages:
		nums.append(int(page.xpath('@data-page')[0]))
	return max(nums)

def return_file(country,final):
	with open('qyer_city_list','a') as f:
		f.write('<---country--->'+country[0]+','+country[1]+'\n')
		for i in final:
			f.write(i[0]+','+i[1]+'\n')


def process_country(country_list):
	final = []
	for country in country_list:
		print 'prcess--->',country[0],country[-1]
		url = country[-1]
		root = request(url +'citylist-0-0-1/')
		result = get_city(root)
		page_max = get_max_page_num(root)
		if page_max >0:
			for i in range(2,1+page_max):
				purl = url + 'citylist-0-0-{}/'.format(str(i))
				result += get_city(request(purl))
		return_file(country,result)
		print 'prcess over--->',country[0]
		

if __name__ == '__main__':
	process_country(get_country())
	