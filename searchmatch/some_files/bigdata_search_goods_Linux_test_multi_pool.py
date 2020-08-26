# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 16:35:53 2019

@author: lijiangman
"""
import json
import sys
import multiprocessing
import time
sys.path.append("../") ## 会从上一层目录找到一个package
#import urllib
#import urllib2
import requests
##多进程，多次循环，才能显示出性能
##谢宇博士写的！
def search_test_mult(text_input,ip):
    shopCode="-1"##默认所有小区
    areaCode='2018012300014'
    sortMethod='3'
    url = "http://"+ip+":8870/searchgoods?"
    url=url+"shopCode="+shopCode+"&areaCode="+areaCode+"&sortMethod="+sortMethod+"&page=1&rows=50&searchKey="+text_input
    r0 = requests.get(url)
    page_dict=r0.json()
    return page_dict
def test():
    text_input="苹果"
    text_input="草莓"
    page_dict=search_test_mult(text_input) 
    print(page_dict)
def main(text_input_list,ip):
    n_proc = 4
#    n_proc = 8
    llist = []
    res_list = []
    res_list2 = []
#    text_input_list = ['苹果', '草莓', '葡萄', '西瓜']
    pool = multiprocessing.Pool(processes = n_proc)
#    for i in range(4):
    index_list=[0,1,2,3]*10
    for i in index_list:
#        apply_async 是异步非阻塞的。
#        意思就是：不用等待当前进程执行完毕，随时根据系统调度来进行进程切换。
        result=pool.apply_async(search_test_mult, args=(text_input_list[i],ip,))
#        print(result.get())
#        llist.append(result)
        res_list.append(result)
#        res_list.append(result.get())
    pool.close()
    pool.join()
    ##必须运行下面的语句，否则时间不会变短！！
    for i in range(len(index_list)):
        res = res_list.pop(0)
        res_list2.append(res.get())
#    print(res_list)
    return res_list2

def main2(text_input_list,ip):
#    text_input_list = ['苹果', '草莓', '葡萄', '西瓜']
    res_list = []
    index_list=[0,1,2,3]*10
    for i in index_list:
#    for i in range(4):
        res_list.append(search_test_mult(text_input_list[i],ip))
#    print(res_list) 
    return res_list


def get_time_of_code(fun,text_input_list,ip):
    t0 = time.time()
    localtime = time.asctime( time.localtime(t0) )
    print(localtime)
#    main()
    res_list=fun(text_input_list,ip)
#    print(res_list)
    t1 = time.time()
    localtime = time.asctime( time.localtime(t1) )
    print(localtime)
    time_used = t1 - t0
    print("Time consumed(s): ", time_used)
    
if __name__ == '__main__':
    text_input_list = ['苹果', '草莓', '葡萄', '西瓜']
#    ip="10.18.226.25"    
    ip="10.18.222.105"##thread pool
    get_time_of_code(main,text_input_list,ip)
#    get_time_of_code(main2,text_input_list,ip)
#    t0 = time.time()
#    localtime = time.asctime( time.localtime(t0) )
#    print(localtime)
#    main2()
#    t1 = time.time()
#    localtime = time.asctime( time.localtime(t1) )
#    print(localtime)
#    time_used = t1 - t0
#    print("Time consumed(s): ", time_used)


