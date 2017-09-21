#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@author: vassago
@date: 2017-09-21
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
    root = request('https://www.tripadvisor.cn/Lvyou')
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
	with open('daodao_country_list','a') as f:
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
	#process_country(get_country())
    get_country()
	
