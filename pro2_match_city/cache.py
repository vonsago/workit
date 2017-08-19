#!/usr/bin/python
# -*- coding: UTF-8 -*-

'''
Created on 2016年12月27日

@author: dujun
'''
from mioji.common.utils import setdefaultencoding_utf8

setdefaultencoding_utf8()
import hashlib
import zlib
import time
from os import path
import os
import json

from func_log import func_time_logger
from logger import logger

cache_dir = path.abspath(path.join(path.dirname(__file__) + '../../../../../cache'))


def has_dir():
    return path.isdir(cache_dir)


@func_time_logger
def get(cache_config, req):
    if not has_dir():
        return None, None

    if not cache_config or not req:
        return None, None

    if cache_config['enable']:
        file_path_str, md5 = file_path(req)
        if not path.isfile(file_path_str):
            return None, None

        file_info = os.stat(file_path_str)
        if file_info.st_size == 0:
            return None, None
        if time.time() - file_info.st_mtime > cache_config['lifetime_sec']:
            return None, None

        fo = open(file_path_str, "r+")
        res = fo.read()
        length1 = len(res)
        fo.close()
        res = zlib.decompress(res)
        length2 = len(res)
        logger.debug("%s, %s, %s", length1, length2, length1 / float(length2))
        return res, md5
    else:
        return None, None


def put(cache_config, req, res):
    if not has_dir():
        return None

    if not cache_config or not req or not res:
        return None

    #     if cache_config['enable']:
    file_path_str, md5 = file_path(req)
    fo = open(file_path_str, "wb")
    res = zlib.compress(res)
    fo.write(res)
    fo.close()
    return md5


def get_by_md5(md5):
    if not has_dir():
        return
    fo = open(cache_dir + '/' + md5, "r+")
    res = fo.read()
    length1 = len(res)
    fo.close()
    res = zlib.decompress(res)
    length2 = len(res)
    print length1, length2, length1 / float(length2)
    return res


def file_path(req):
    md5_name = md5(json.dumps(req, sort_keys=True))
    print 'md5', md5_name, 'req', req
    return cache_dir + '/' + md5_name, md5_name


def md5(src):
    m2 = hashlib.md5()
    m2.update(src)
    return m2.hexdigest()


if __name__ == '__main__':
    #     put({'enable':True}, {'a':11}, 'sdfalksdfj')
    print get_by_md5('72baee7dd1fb6d8767179a3a24149a3b')