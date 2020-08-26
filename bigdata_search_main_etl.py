# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 10:52:40 2020

@author: lijiangman
"""

import sys
from server.bigdata_search_goods_multi_executors_etl_test import para_set, initPickle, start


argv=sys.argv[1:]
confPath=para_set(argv)

print('init pickle')
initPickle(confPath)

print('start service')
start(confPath)



