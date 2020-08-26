# -*- coding: utf-8 -*-
"""
Created on Thu May 21 14:41:14 2020

@author: lijiangman
"""

import time
from apscheduler.schedulers.blocking import BlockingScheduler

def job(clean=False):
    print("processing-------------------------")

    print(clean)
    if clean:
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    else:
        print("hehe")


def hot_search_schedule():
    # 该示例代码生成了一个BlockingScheduler调度器，使用了默认的任务存储MemoryJobStore，以及默认的执行器ThreadPoolExecutor，并且最大线程数为10。
    
    # BlockingScheduler：在进程中运行单个任务，调度器是唯一运行的东西
    scheduler = BlockingScheduler()
    # 采用阻塞的方式
    print("start-------------------------")

    # 采用固定时间间隔（interval）的方式，每隔5秒钟执行一次
    # scheduler.add_job(job, args=[False], trigger='interval', seconds=10)
    scheduler.add_job(job, args=(False,), trigger='interval', seconds=10)
    # scheduler.add_job(job, trigger='interval', seconds=10)

    scheduler.start()
        

if __name__ == '__main__':
    
    hot_search_schedule()
    
    
    
    
    
    
    
    