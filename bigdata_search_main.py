# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 10:52:40 2020

@author: lijiangman
"""

import multiprocessing
from server.bigdata_search_goods_multi_executors import start
from searchmatch.dianshang_seg_lib import initPickle
from searchhotmain.index_create import init_hot_search_test
from searchhotmain.index_schedule import hot_search_schedule
from searchmatch.index_update_cre import iu
from searchmatch.index_schedule_cre import incre_update_schedule
from searchmatch.loggerbyme import logger_search

# from searches.esclient_byme import create_index_sense, index_sense
# logger = logging.getLogger()    # initialize logging class
# logger.setLevel(logging.INFO)

# argv=sys.argv[1:]
# confPath=para_set(argv)

# print('init pickle')
# logger_search.info('initing pickle')
# initPickle()

# logger_search.info('initing es index')
# iu.index_my_mysql(clean=True)

"""
多进程
"""


# logger_search.info('multi tasks')


def task1():

    logger_search.info('I am task1')

    logger_search.info('Initialing hot words...')
    init_hot_search_test()
    logger_search.info('Finish hot words')

    """
    定时任务
    """
    logger_search.info('Start schedule of initialing hot words')
    hot_search_schedule()

#
# def task2():
#     """
#     web服务
#     """
#     logger_search.info("I am task2")  # 这是主进程的任务
#
#     logger_search.info('Initialing pickle words...')
#     initPickle()
#     logger_search.info('Finish pickle words')
#
#     logger_search.info('start web service')
#     start()


def task3():
    """
    定时任务
    """
    logger_search.info('I am task3')

    logger_search.info('Initialing whoosh index...')
    iu.index_my_mysql(clean=True)
    logger_search.info('Finish whoosh index')

    logger_search.info('Start schedule of incremental update search')
    incre_update_schedule()


# def task4():
#     """
#     初始化敏感词
#     """
#     logger_search.info('I am task4')
#
#     logger_search.info('Initing es index min_gan_ci')
#     logger_search.info("Creating an index...")
#     create_index_sense()
#     logger_search.info("Index min_gan_ci documents...")
#     index_sense()
#     logger_search.info('Finish es index min_gan_ci')


p1 = multiprocessing.Process(target=task1)  # multiprocessing.Process创建了子进程对象p1
# p2 = multiprocessing.Process(target=task2)  # multiprocessing.Process创建了子进程对象p2
p3 = multiprocessing.Process(target=task3)  # multiprocessing.Process创建了子进程对象p3
# p4 = multiprocessing.Process(target=task4)  # multiprocessing.Process创建了子进程对象p4

p1.start()  # 子进程p1启动
# p2.start()  # 子进程p2启动
p3.start()  # 子进程p3启动
# p4.start()  # 子进程p4启动


logger_search.info("I am main task")  # 这是主进程的任务

logger_search.info('Initialing pickle words...')
initPickle()
logger_search.info('Finish pickle words')

logger_search.info('Start web service and init')
start()







