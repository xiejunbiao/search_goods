# -*- coding: utf-8 -*-
"""
Created on Fri May  8 10:00:19 2020

@author: lijiangman
"""

import time

def consumer(name):
    print('%s准备吃包子了！'% name)
    while(True):
        baozi = yield
        print('包子%s来了，被%s吃了！'%(baozi,name))

def producer(name):
    c = consumer('a')
    c2 = consumer('b')
    c.__next__()
    c2.__next__()
    print('老子要吃包子了')
    for i in range(5):
        time.sleep(1)
        print('做了两个包子')
        c.send(str(i)+'a')
        c2.send(str(i)+'b')
        pass
#通过send 方法向yield传输值；yield通过send接受值；
#也就是生产者向消费者传递物品；
#串行中实现异步的过程；
producer('producer')