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
        print '----VON ----start targets request'
        @request(retry_count=3, proxy_type=PROXY_REQ,binding = self.parse_room)
        def first_page():
            return {
                'req' :{'url': self.request_url,},

            }
        print '----VON ----end targets request'
        yield first_page

    def respon_callback(self, req, resp):
        pass

    def parse_room(self, req, resp):
        room = Room()
        rooms =[]
        sresp=resp.encode('utf-8')
        try:
            room.source = self.source
            room.real_source = self.source
            room.check_in = '-'.join([self.arrivalYear,self.arrivalMonth,self.arrivalDay])
            room.hotel_name = re.findall(r'class="hotelNameNoLink">(.*?)<',sresp)[0]
            room.hotel_city = self.city
            room.source_hotelid=self.ctyhocn

            choice_data = re.findall(r'<span>选择</span><span class="hidden">(.*?)<',sresp)#弹性房价房价，每晚 1,210 人民币，豪华房（特大床）。
            price_data = re.findall(r'priceamount currencyCode-(...)',sresp)#'CNY',
            id_data = re.findall(r'&_eventId=rateSummaryDetailsPopup&srpId=(.*?)&roomCode=(.*?)&',sresp)
            out_day = re.findall(r'<span class="sumDates">(.*?)</span>',sresp)[0]
            out_day =out_day.split('－')[-1]
            out_day = ''.join(re.findall('\d',out_day))
            out_day = ''.join(out_day[0:4])+'-'+''.join(out_day[4:6])+'-'+''.join(out_day[-2:])
            room.check_out = out_day
            for data in choice_data:
                i =choice_data.index(data)
                room.source_roomid= id_data[i]
                lis = data.split('，')
                room.room_type = lis[-1]+','+lis[0]
                price = re.findall('\d',lis[1])
                price = float(''.join(price))
                room.price = price
                room.tax = -1
                room.rest = -1
                room.floor = -1
                room.size = -1  
                room.occupancy = 1 
                room.currency = price_data[i]
                room.pay_method = 'mioji'
                room.is_extrabed ='Null' 
                room.is_extrabed_free ='Null'  
                room.has_breakfast = 'Null'
                room.is_breakfast_free ='Null'  
                room.is_cancel_free = 'Null'  
                room.extrabed_rule = 'Null'  
                room.return_rule = 'Null'  
                room.change_rule = 'Null'  
                room.room_desc = 'Null'  
                room.others_info = 'Null'  
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

            print len(rooms)
            return rooms
        except Exception as e:
            print 'parse error'
            print e

if __name__ == '__main__':
    from mioji.common.task_info import Task
    task = Task()
    task.content = 'Hilton Birmingham Metropole&Birmingham&BHXMETW&20171030'
    spider = hiltonHotelSpider(task)
    print spider.crawl()
    print spider.result
    print spider.verify_tickets
