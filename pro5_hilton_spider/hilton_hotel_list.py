#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mioji.common.utils import setdefaultencoding_utf8
import unicodedata

setdefaultencoding_utf8()
import re
import json
import urllib
from mioji.common import parser_except
from mioji.common.spider import Spider, request, PROXY_FLLOW, PROXY_REQ
from mioji.common.class_common import Room  


url_template = 'http://www.hilton.com.cn/zh-cn/city/{}-hotels.html'

class hiltonHotelSpider(Spider):
    source_type = 'hiltonHotel'
    targets = { 
        # 例行需指定数据版本：InsertHotel_room4
        'hotel':{},
            } 

    def __init__(self, task=None):
        Spider.__init__(self)
        # 处理这些信息
        if task:
            self.task = task
        self.city = task.content
        self.request_url = url_template.format(self.city) 

        # 爬取的某些参数
        self.ids = None
        self.verify_flights = []  # 这个是验证时保存的flight

        self.tickets = []
        self.verify_tickets = []  # 这个是验证结果出来保存的票

    def targets_request(self):
        print '----VON ----start targets request'
        @request(retry_count=3, proxy_type=PROXY_REQ,binding = self.parse_hotel)
        def first_page():
            return {
                'req' :{'url': self.request_url,},
            }
        print '----VON ----end targets request'
        yield first_page

    def respon_callback(self, req, resp):
        pass
    
    def parse_hotel(self, req, resp):
        print '----VON----start parse room'
        try:
            result = []
            pat = re.compile(r'var hotelJsonList = (.*);.*var Country',re.S)
            data = pat.findall(resp)[0]
            dic_lis = json.loads(data)
            for dic in dic_lis:
                result.append({'ID':dic['ID'],'HotelType':dic['HotelType'],'HotelName':dic['HotelName'],'HotelCode':dic['HotelCode'],'RealUrl':dic['RealUrl']})
            return result
        except Exception as e :
            print 'error - error - error - error - error'
            print e ,type(dic)

if __name__ == '__main__':
    from mioji.common.task_info import Task
    task = Task()
    task.content = 'beijing'
    spider = hiltonHotelSpider(task)
    print spider.crawl()
    print spider.result
    print spider.verify_tickets
