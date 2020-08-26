# -*- coding: utf-8 -*-
"""
Created on Sun May 24 17:36:26 2020

@author: lijiangman
"""
import logging
##输出到屏幕，谢谢翟博士
def getLogger():
    # strPrefix = "%s%d%s" % (strPrefixBase, os.getpid(),".log")##进程的pid命名日志
    logger = logging.getLogger("SEARCH_GOODS")
    logger.propagate = False
    #handler = TimedRotatingFileHandler(strPrefix, 'H', 1)
    ##将http访问记录，程序自定义日志输出到文件，按天分割，保留最近30天的日志。
    # 使用TimedRotatingFileHandler处理器
#    handler = TimedRotatingFileHandler(strPrefix, when="d", interval=1, backupCount=60)##d表示按天
#    interval 是间隔时间单位的个数，指等待多少个 when 的时间后 Logger 会自动重建新闻继续进行日志记录
#    handler = TimedRotatingFileHandler(strPrefix, when="midnight", interval=0, backupCount=60)##
#    atTime=Time_log_info(23,59,59)
#    handler = TimedRotatingFileHandler(strPrefix, when="midnight", interval=0, backupCount=60, atTime=atTime)##
    #实例化TimedRotatingFileHandler
    #interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
    # S 秒、 M 分、 H 小时、 D 天、W 每星期（interval==0时代表星期一）、 midnight 每天凌晨
    ##
    # handler=SafeFileHandler(filename=strPrefix,mode='a')
#    handler.suffix = "%Y%m%d_%H%M%S.log"
    ##2020-01-10 10:36:54,485 - SEARCH_GOODS - INFO - 格式
    handler=logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger