#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mioji.common.utils import setdefaultencoding_utf8
import unicodedata

setdefaultencoding_utf8()
import re
import json
import urllib
from lxml import html as HTML
from mioji.common import parser_except
from mioji.common.spider import Spider, request, PROXY_FLLOW, PROXY_REQ
from mioji.common.class_common import Room  


url_template = 'https://secure3.hilton.com/zh_CN/hi/reservation/book.htm?ctyhocn={0}&inputModule=HOTEL_SEARCH&arrivalDay={1}&arrivalMonth={2}&arrivalYear={3}' 

class hiltonHotelSpider(Spider):
    source_type = 'hiltonHotel'
    targets = { 
        # 例行需指定数据版本：InsertHotel_room4
        'room': {'version': 'InsertHotel_room3'},
            } 

    def __init__(self, task=None):
        Spider.__init__(self)
        # 处理这些信息
        if task:
            self.task = task
        lis = task.content.split('&')
        day = lis[-1]
        self.source = 'hiltonHotel'
        self.city = lis[1]
        self.ctyhocn = lis[2]
        self.arrivalDay = day[-2:]
        self.arrivalMonth = day[-4:-2]
        self.arrivalYear = day[:4]
        self.request_url = url_template.format(self.ctyhocn,self.arrivalDay, self.arrivalMonth, self.arrivalYear) 

        # 爬取的某些参数
        self.ids = None
        self.verify_flights = []  # 这个是验证时保存的flight

        self.tickets = []
        self.verify_tickets = []  # 这个是验证结果出来保存的票

    def targets_request(self):
        @request(retry_count=3, proxy_type=PROXY_REQ,binding = self.parse_room)
        def first_page():
            return {
                'req' :{'url': self.request_url,},

            }
        yield first_page

    def respon_callback(self, req, resp):
        pass

    def parse_room(self, req, resp):
        room = Room()
        rooms =[]
        sresp=resp.encode('utf-8')
        root = HTML.fromstring(resp)
        try:
            room.source = self.source
            room.real_source = self.source
            room.check_in = '-'.join([self.arrivalYear,self.arrivalMonth,self.arrivalDay])
            date_info = root.xpath('//span[@class="sumSectionDatesDesktop"]/span[@class="sumDates"]')[0].text_content()
            room.check_out = date_info.split('-')[-1].replace(u'年',u'-').replace(u'月',u'-').replace(u'日','').replace(' ','')
            room.hotel_name = root.xpath('//h1[@class="hotelNameNoLink"]')[0].text_content()
            room.hotel_city = self.city
            room.source_hotelid=self.ctyhocn

            choice_data = root.xpath('//ul[@class="group"]/li') 
            for data in choice_data:

                cu = data.xpath('div[@class="optionItems"]/h6/span[@class="priceHeader"]')[0].text_content()
                room.currency=re.findall(u'[\(\（](.*?)\)',cu)[0]
                #print room.currency
                try:
                    room.tax = float(re.findall(u'([\d\.]*\%)',cu)[0][:-1])/100
                    #print 'tax--->',room.tax
                except:
                    room.tax=-1
                
                desc = data.xpath('div[@class="itemTitleAndDesc"]/span')[0].text_content().replace('\n','').replace(' ','').replace('\t','')
                more_list = data.xpath('div[@class="optionItems"]//li')
                for more in more_list:
                    source_roomid_href = more.xpath('div[@class="rate-desc-wrapper"]/div/strong/a')[0].get('href')
                    source_roomid = re.findall('&srpId=(.*?)&roomCode=(.*?)&',source_roomid_href)[0]
                    room.source_roomid = source_roomid[0]+','+source_roomid[1]
                    more = more.text_content().replace('\n','').replace(' ','').replace('\t','')
                    #print 'more--->',more
                    _more = more.split('。')
                    room.room_type = _more[0].replace('详情','').replace('房价类型','')
                    room.price = re.findall(u'每晚([\d,]*)',_more[-2])[0].replace(',','')
                    room.rest = -1
                    room.floor = -1
                    room.occupancy = 1
                    room.pay_method = 'mioji'
                    room.is_extrabed ='Null'
                    room.is_extrabed_free ='Null'
                    room.has_breakfast = 'Null'
                    room.is_breakfast_free ='Null' 
                    room.is_cancel_free = 'Null'
                    room.extrabed_rule = 'Null'
                    room.return_rule = 'Null'
                    room.change_rule = 'Null' 
                    room.others_info = 'Null'
                    room.room_desc = desc + _more[1] 
                    #print 'desc---->>->>',room.room_desc
                    room_tuple = (str(room.hotel_name), str(room.city), str(room.source),str(room.source_hotelid),
                            str(room.source_roomid),str(room.real_source), str(room.room_type), int(room.occupancy), 
                            str(room.bed_type), float(room.size), int(room.floor), str(room.check_in), 
                            str(room.check_out), int(room.rest), float(room.price), float(room.tax), 
                            str(room.currency), str(room.pay_method), str(room.is_extrabed), str(room.is_extrabed_free), 
                            str(room.has_breakfast), str(room.is_breakfast_free), 
                            str(room.is_cancel_free), str(room.extrabed_rule), str(room.return_rule),
                            str(room.change_rule),  
                            str(room.room_desc), str(room.others_info), str(room.guest_info))
                    rooms.append(room_tuple) 
            return rooms
        except Exception as e:
            print 'parse error'
            print e

if __name__ == '__main__':
    from mioji.common.task_info import Task
    task = Task()
    task.content ='Hilton Berlin&Berlin&BERHITW&20171108'
    spider = hiltonHotelSpider(task)
    print spider.crawl()
    print spider.result
    print spider.verify_tickets
