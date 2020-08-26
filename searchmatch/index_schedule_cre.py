# -*- coding: utf-8 -*-
"""
Created on Thu May 21 14:41:14 2020

@author: lijiangman
"""

import time
from apscheduler.schedulers.blocking import BlockingScheduler
# from searchhotmain.index_create import init_hot_search_test
from searchmatch.index_update_cre import iu


def job(clean=False):
    print("processing-------------------------")
    if clean:
        print(clean)
    else:
        print(clean)

    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


def incre_update_schedule():
    # 该示例代码生成了一个BlockingScheduler调度器，使用了默认的任务存储MemoryJobStore，以及默认的执行器ThreadPoolExecutor，并且最大线程数为10。
    
    # BlockingScheduler：在进程中运行单个任务，调度器是唯一运行的东西
    scheduler = BlockingScheduler()
    # 采用阻塞的方式
    # print("start incre_update-------------------------")

    # 采用固定时间间隔（interval）的方式，每隔5秒钟执行一次
    # scheduler.add_job(job, 'interval', seconds=60)
    """
    WARNING:apscheduler.scheduler:Execution of job 
    "init_hot_search (trigger: interval[0:01:00], next run at: 2020-05-21 21:13:59 CST
    
    时间间隔太短，会报警
    """
    # scheduler.add_job(iu.index_my_mysql, args=(False,), trigger='interval', seconds=10800)##30*60---不能把False当成字符串！
    # scheduler.add_job(job, args=(False,), trigger='cron', hour='16', minute='42')
    # 在每天22和23点的25分，运行一次 job 方法
    scheduler.add_job(iu.index_my_mysql, args=(True,), trigger='cron', hour='5', minute='50')##改为全量，因为增量的时候，可能更新时间都改了（刘尊彦全量更新），时间很长

    # scheduler.add_job(iu.index_my_mysql, args=('False',), trigger='interval', seconds=1800)##30*60
    # scheduler.add_job(init_hot_search_test, 'interval', seconds=300)##--测试
    
    scheduler.start()
        

if __name__ == '__main__':
    
    incre_update_schedule()
    
    
    
    
    
    
    
    