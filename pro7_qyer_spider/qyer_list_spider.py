#!/usr/bin/python
# -*-coding:UTF-8 -*-

import sys
from math import ceil
from collections import defaultdict
import re
from urlparse import urljoin
import requests
from lxml.html import tostring
#
#and other needed private import 
#
reload(sys)
sys.setdefaultencoding('utf-8')

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'place.qyer.com',
    'Upgrade-Insecure-Requests': '1'
}


class QyerSpiderApi(Spider):
    # 抓取目标：穷游指定城市景点，美食，购物等信息列表
    source_type = 'qyerListInfo'

    # 数据目标：景点，美食，购物，活动的全部URL
    targets = {
        'list': {}
    }

    old_spider_tag = {
        'qyerList': {'required': ['list']}
    }

    def __init__(self, task=None):
        super(QyerSpiderApi, self).__init__(task)
        self.headers = headers
        self.type = None
        self.city_url = None
        self.city_name = None
        self.url_type = None
        self.save_city_page_url = defaultdict(list)
        self.save_city_info = defaultdict(dict)
        self.types = {
            'sight': 'attr',
            'food': 'rest',
            'shopping': 'shop',
            'activity' :'acti'
        }
        self.types_result_num = {
            'attr': -1,
            'rest': -1,
            'shop': -1,
            'acti': -1
        }
        self.tags = {
            'sight': [],
            'food': [],
            'shopping': [],
            'activity': []
        }
        self.tags_url ={
            'sight': [],
            'food': [],
            'shopping':[],
            'activity':[]
        }
        self.tag_page ={
            'sight': {},
            'food': {},
            'shopping':{},
            'activity':{}
        }

    def targets_request(self):
        tid = 'demo'
        used_times =3

        @request(retry_count=5, proxy_type=PROXY_REQ, user_retry_count=10,
                 store_page_name="city_first_page_{}_{}".format(tid, used_times))
        def city_first():
            url = self.task.content
            crawl_type = []
            for type in self.types.keys():
                crawl_type.append(
                    {
                        'req': {
                            'url': urljoin(url, type),
                            'headers': headers
                        },
                        'data': {
                            'content_type': 'html'
                        },
                        'user_handler': [self.get_type_info]
                    }
                )
            return crawl_type

        yield city_first

        @request(retry_count=5, proxy_type=PROXY_REQ, user_retry_count=10,
                store_page_name="city_first_page_{}_{}".format(tid, used_times))
        def tag_pages():
            url = 'http://place.qyer.com/' 
            crawl_tag = []
            for type in  self.types.keys():
                for tag_url in self.tags_url[type]:
                    crawl_tag.append(
                    {
                            'req':{
                            'url': urljoin(url,tag_url),
                            'headers': headers
                            },
                        'data': {
                        'content_type': 'html'
                        },
                        'user_handler': [self.get_page_info]
                        }
                    )
            return crawl_tag
        yield tag_pages

        @request(retry_count=5, proxy_type=PROXY_REQ, binding=['list'], user_retry_count=10,
                 store_page_name="city_branch_pages_{}_{}".format(tid, used_times))
        def branch_pages():
            city_page = []
            for city_name in self.save_city_info.keys():
                for type in self.save_city_info[city_name].keys():
                    for tag in self.save_city_info[city_name][type][-1].keys():
                        pages = []
                        for page in range(1, self.tag_page[type][tag] + 1):
                            pages.append(
                                {
                                    'req': {
                                        'url': 'http://place.qyer.com/poi.php?action=list_json',
                                        'data': {
                                            'page': page,
                                            'type': self.url_type,
                                            'pid': self.save_city_info[city_name][type][2],
                                            'sort': self.save_city_info[city_name][type][1],
                                            'subsort': tag,
                                            'isnominate': '-1',
                                            'haslastm': 'false',
                                        # 'rank': 6
                                            'rank': '0'
                                        },
                                        'method': 'post',
                                        #'city_name': city_name,
                                        'type': type,
                                    },
                                    'data': {'content_type': 'json'},
                                }
                            )
                        city_page.extend(pages)
            print "city_page_length:", len(city_page)
            print city_page
            return city_page

        yield branch_pages
        print "抓取url:", self.save_city_page_url

    def get_page_info(self, req, data):
        type = req['req']['url'].split('/')[-3]
        tag = req['req']['url'].split('/')[-2][3:]
        root = data
        try:
            self.tag_page[type][int(tag)] =int(root.xpath('//div[@id="poiListPage"]/div/a')[-2].xpath('@data-page')[0])
        except Exception as e:
            # one page
            self.tag_page[type][int(tag)] = 1
        print '----max_page----',self.tag_page[type][int(tag)]


    def get_type_info(self, req, data):

        if '''TYPE:'country',''' in tostring(data):
            self.url_type = 'country'
        else:
            self.url_type = 'city'
        root = data
        type = req['req']['url'].split('/')[-1]
        self.type = type
        city_name = req['req']['url'].split('/')[-2]
        self.city_name = city_name
        if type == 'sight':
            page_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-sight"]')[0].text
            total_num = int(re.findall(r'\d+', page_info)[0])
            self.types_result_num['attr'] = total_num
            page = int(ceil(total_num / 15.0))
            sort_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-sight"]')[0]
            sort = int(sort_info.xpath('@data-id')[0])
            pid_info = root.xpath('//script[@type="text/javascript"]')[0].text
            pid = int(re.findall(r'\d+', pid_info)[0])
        if type == 'food':
            page_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-food"]')[0].text
            total_num = int(re.findall(r'\d+', page_info)[0])
            self.types_result_num['rest'] = total_num
            page = int(ceil(total_num / 15.0))
            sort_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-food"]')[0]
            sort = int(sort_info.xpath('@data-id')[0])
            pid_info = root.xpath('//script[@type="text/javascript"]')[0].text
            pid = int(re.findall(r'\d+', pid_info)[0])
        if type == 'shopping':
            # page_info = root.xpath('//a[@data-bn-ipg="place-city-poi-moreshopping"]')[0].text
            page_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-shopping"]')[0].text
            total_num = int(re.findall(r'\d+', page_info)[0])
            self.types_result_num['shop'] = total_num
            page = int(ceil(total_num / 15.0))
            sort_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-shopping"]')[0]
            sort = int(sort_info.xpath('@data-id')[0])
            pid_info = root.xpath('//script[@type="text/javascript"]')[0].text
            pid = int(re.findall(r'\d+', pid_info)[0])
        if type == 'activity':
            page_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-activity"]')[0].text
            total_num = int(re.findall(r'\d+', page_info)[0])
            self.types_result_num['acti'] = total_num
            page = int(ceil(total_num /15.0))
            sort_info = root.xpath('//a[@data-bn-ipg="place-poilist-filter-classify-activity"]')[0]
            sort = int(sort_info.xpath('@data-id')[0])
            pid_info = root.xpath('//script[@type="text/javascript"]')[0].text
            pid = int(re.findall(r'\d+', pid_info)[0])


        self.save_city_info[city_name][type] = [page, sort, pid]

        #------------
        print '------VON-----'
        tag_list = root.xpath('//p[@id="poiSortLabels"]/a')
        ta = {}
        for tag in tag_list :
            if int(tag.xpath('@data-id')[0]) != 0:  # 
                self.tags[type].append(tag.text)
                self.tags_url[type].append(tag.xpath('@href')[0])
                ta[int(tag.xpath('@data-id')[0])] = tag.text
        self.save_city_info[city_name][type].append(ta)
        #print self.tags
        #print self.tags_url
        #-----------
        print '------====save_city_info====------',self.save_city_info

    def parse_list(self, req, data):
        if data['result'] != 'ok':
            raise ParserException(parser_error_code=ParserException.PROXY_INVALID)
        temp_list = []
        for list in data['data']['list']:
            temp_list.append((
                list['id'],
                urljoin('http:', list['url']),
                req['req']['data']['page'],
                req['req']['type'],
                self.save_city_info[self.city_name][req['req']['type']][-1][req['req']['data']['subsort']]
            ))
        return temp_list



if __name__ == "__main__":
    import mioji.common.spider
    from mioji.common.utils import simple_get_socks_proxy
    from mioji.common import spider

    spider.slave_get_proxy = simple_get_socks_proxy
    mioji.common.spider.NEED_FLIP_LIMIT = False

    task = Task()
    task.content = 'http://place.qyer.com/tokyo/'
    # task.content = 'http://place.qyer.com/paris/'
    task.content = 'http://place.qyer.com/new-york/'
    task.content = 'http://place.qyer.com/paris/'
    task.content = 'http://place.qyer.com/taif/'
    task.content = 'http://place.qyer.com/praslin-island/'
    task.content = 'http://place.qyer.com/bavaria/'
    task.content = 'http://place.qyer.com/lake-district-national-park/'
    #task.content = 'http://place.qyer.com/albania/'
    #task.content = 'http://place.qyer.com/seoul/'
    task.ticket_info = {
        'tid': 'demo',
        'used_times': 3
    }
    spider.task = task
    # spider = NewQyerSpider(task)
    spider = QyerSpiderApi(task)
    spider.crawl()
    print('=================================================================================0')
    res = spider.result['list']
    print res
    print(spider.page_store_key_list)
    print(spider.types_result_num)
    print len(res)
    

    print('=================================================================================1')