# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 15:01:02 2019

@author: lijiangman
"""

##为了配合冯超和王莉进行数据清洗，另起一个etl测试服务
##多线程
#增加  @run_on_executor


##更改日志配置--不适用于多进程
##修改日志方案

import getopt

import os
import sys
import json
import traceback

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.httpclient
import tornado.web
import tornado.gen
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
#####
import logging
#from logging.handlers import TimedRotatingFileHandler
logging.basicConfig()

##参考https://www.jianshu.com/p/d615bf01e37b
# from logging import FileHandler
# import codecs
# import time
######

# sys.path.append("../") ## 会从上一层目录找到一个package
# sys.path.append("/opt/ado-services/bigdata-search/bigdata-search/")

from searchMatchV2.search_for_xwj import search_main, search_query_revise #模块名字不能有小数点
from searchMatchV2.dianshang_seg_lib import initPickle

# from searchMatchV2.utils.logHandle import SafeFileHandler

from searchMatchV2.utils.configInit import ConfigValue##数据库配置类

#from timeClassModul import Time_log_info##我自己定义的时间类

#g_log_prefix = '../log/rela_baike_tornado.'
# g_log_prefix = './search_goods.'##当前目录
# g_log_prefix = './logs/search_goods.'##目录
# g_log_prefix = '/var/log/searchgoodshttp/search_goods.'#绝对路径
# g_log_prefix_revise = '/var/log/searchgoodshttp/search_query_revise.'#绝对路径



####

            
##为了定义日志的时间格式，写一个时间类的模块
#以下定义了一个类，类名为Person_info，里面有两个参数，分别是name和age
# class Time_log_info:
#     def __init__(self,hour,minute,second):
        
#         self.hour=hour
#         self.minute=minute
#         self.second=second

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


class SearchGoodsHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)
    def initialize(self, logger, config_result):##删除一个输入参数
#        self.__serverRelaBaike = relaServer
        self.__logger = logger
        self.config_result = config_result

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        query = self.get_argument('searchKey')
        pageNum = self.get_argument('page')
        rows = self.get_argument('rows')#每页有多少行
        areaCode=self.get_argument('areaCode')#
        shopCode=self.get_argument('shopCode',default='-1')#改成默认--好像在浏览器中必须输入数字，不能输入字符--redis缓存做key的时候，改
#        shopCode=self.get_argument('shopCode',"")#改成默认是空-报错--500: Internal Server Error
        sortMethod=self.get_argument('sortMethod',default='-1')#非必须--默认是-1  改为8888ljm也行
        
#        page_json=search_main(text_input,pageNum,rows,areaCode,shopCode,sortMethod)
        arg_dict={}
        arg_dict['query']=query
        arg_dict['page']=pageNum
        arg_dict['rows']=rows
        arg_dict['areaCode']=areaCode
        arg_dict['shopCode']=shopCode
        arg_dict['sortMethod']=sortMethod
        
        # arg_dict['config_result']=self.config_result#数据库配置的路径文件
        
        #必须加self，否则找不到此函数
        page_json=yield self.get_search_main(arg_dict)
        
        self.write(page_json)

#    def __logResponse(self, utfQuery, relaResult):
#        succ = relaResult.isSuccess()
#        if succ:
#            self.__logger.info("%s\tSucc\t%s" % (utfQuery, "|".join([str(item[0]) for item in relaResult])))
#        else:
#            self.__logger.info("%s\tError:%d" % (utfQuery, relaResult.getError()))

    @run_on_executor
    def get_search_main(self, arg_dict):#也必须输入self
        # try:
        #     page_json=search_main(text_input,pageNum,rows,areaCode,shopCode,sortMethod)
        #     ##现在报错只能通过nohup来打印日志，加一个异常捕捉
        # except Exception as e:
        #     sinfo='failed:'+str(e)
        
        
        # 如何把异常写进日志？
        try:
            
            # input_dict={'searchKey':text_input,'page':pageNum,'rows':rows,'areaCode':areaCode,'shopCode':shopCode,'sortMethod':sortMethod}
            input_json=json.dumps(arg_dict, ensure_ascii=False)##False解决中文乱码
        
            self.__logger.info("input_dict-"+input_json)##用input_dict-作为分隔符

            page_json=search_main(arg_dict)

            self.__logger.info("page_json-"+page_json)

        except Exception as e:
            self.__logger.info("error:")##用input_dict-作为分隔符
            self.__logger.info(e)##用input_dict-作为分隔符
            self.__logger.info("traceback My:")##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
            page_json={}

        return page_json

        
        
            
        
#        log.logger.info(input_json)
#        log.logger.info(page_json)
        
        
        

        # return page_json
    
#    def getRelaBaike(self, utfQuery):
#        error = 0
#        lstSummary = []
#        relaBaikeRequest = RelaBaikeRequest(content=utfQuery)
#        relaBaikeResult = self.__serverRelaBaike.getRelaBaike(relaBaikeRequest)
#        self.__logResponse(utfQuery, relaBaikeResult)
#        if relaBaikeResult.isSuccess():
#            for item in relaBaikeResult:
#                baikeid = item[0]
#                try:
#                    dicSummary = json.loads(item[1])
#                except:
#                    return -2, 'summary format error' ,lstSummary
#                lstSummary.append(dicSummary)
#        else:
#            return relaBaikeResult.getError(), rela_baike_server.g_dic_error.get(relaBaikeResult.getError(), 'other error') ,lstSumm
#ary
#        return 0, 'success',lstSummary

class SearchReviseHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)
    def initialize(self, logger, testa):##删除一个输入参数
#        self.__serverRelaBaike = relaServer
        self.__logger = logger
        self.testa=testa

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        query = self.get_argument('searchKey')
        # pageNum = self.get_argument('page')
        # rows = self.get_argument('rows')#每页有多少行
        # areaCode=self.get_argument('areaCode')#
        # shopCode=self.get_argument('shopCode',default='-1')#改成默认--好像在浏览器中必须输入数字，不能输入字符--redis缓存做key的时候，改
#        shopCode=self.get_argument('shopCode',"")#改成默认是空-报错--500: Internal Server Error
        # sortMethod=self.get_argument('sortMethod',default='-1')#非必须--默认是-1  改为8888ljm也行
        
        arg_dict={}
        arg_dict['query']=query
        # arg_dict['page']=pageNum
        # arg_dict['rows']=rows
        # arg_dict['areaCode']=areaCode
        # arg_dict['shopCode']=shopCode
        # arg_dict['sortMethod']=sortMethod
        
        
        #必须加self，否则找不到此函数
        page_json=yield self.get_search_main(arg_dict)
        
        self.write(page_json)

#    def __logResponse(self, utfQuery, relaResult):
#        succ = relaResult.isSuccess()
#        if succ:
#            self.__logger.info("%s\tSucc\t%s" % (utfQuery, "|".join([str(item[0]) for item in relaResult])))
#        else:
#            self.__logger.info("%s\tError:%d" % (utfQuery, relaResult.getError()))

    @run_on_executor
    def get_search_main(self, arg_dict):#也必须输入self
        # try:
        #     page_json=search_main(text_input,pageNum,rows,areaCode,shopCode,sortMethod)
        #     ##现在报错只能通过nohup来打印日志，加一个异常捕捉
        # except Exception as e:
        #     sinfo='failed:'+str(e)
        
        
        # 如何把异常写进日志？
        try:
            query_revise=search_query_revise(arg_dict)
            
            # input_dict={'searchKey':text_input,'page':pageNum,'rows':rows,'areaCode':areaCode,'shopCode':shopCode,'sortMethod':sortMethod}
            input_json=json.dumps(arg_dict, ensure_ascii=False)##False解决中文乱码
        
            self.__logger.info("input_dict-"+input_json)##用input_dict-作为分隔符
            self.__logger.info("query_revise-"+query_revise)

        except Exception as e:
            self.__logger.info("error:")##用input_dict-作为分隔符
            self.__logger.info(e)##用input_dict-作为分隔符
            self.__logger.info("traceback My:")##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
            

        return query_revise

def para_set(argv):
   ip = ''
   port = ''
   try:
      opts, args = getopt.getopt(argv,"hc:l:d",["conf=","log=","db="])
   except getopt.GetoptError:
      print('test.py -i <inputfile> -p <outputfile>')
      sys.exit(2)
   for opt, arg in opts:
      print(opt,arg)
      if opt == '-h':
         print('test.py -i <inputfile> -p <outputfile>')
         sys.exit()
      elif opt in ("-c", "--conf"):
         conf = arg
      elif opt in ("-l", "--log"):
         log = arg
      elif opt in ("-d", "--db"):
         db = arg
        
#   self.__logger.info('输入的文件为：', ip)
#   self.__logger.info('输出的文件为：', port)
#   self.__logger.info('db:',db)
   # return (conf,log)
   return (conf)

        
def start(confPath):



    # rootDir='/opt/searchgoodshttp/config.ini'#服务器配置
    # rootDir='D:\hisense\speechR\searchgoodshttp\server\config.ini'#本地配置
    # rootDir='D:\hisense\speechR\searchgoodshttp\server\config53.ini'#本地配置
    cv=ConfigValue(confPath)
    config_result=cv.get_config_values()##全局变量，运行一次，数据库配置。放到内存里面


    
    # g_log_prefix = logPath+'/search_goods.'#绝对路径
    # g_log_prefix_revise = logPath+'/search_query_revise.'#绝对路径

    # port=6601#by me
    port=6611#by me
#    serverRelaBaike = rela_baike_server.getRelaBaikeServer()
    logger = getLogger()
    # logger_revise = getLogger(g_log_prefix_revise)#---目前会同时写入两个log路径

    # 下一步，用Python自带的log，Python输出到屏幕    
    # 修改了bd_goods_spu_search
    # 下一步，把相对路径../改为绝对路径！ sys--用我之前的Python书上面的package包的绝对路径方法！！！
    
    app = tornado.web.Application(handlers=[
        (r"/bigdata-search/search/searchForGoods_etl_test", SearchGoodsHandler,  dict(logger=logger, config_result=config_result))
        ,(r"/bigdata-search/search/searchQueryRevise_etl_test", SearchReviseHandler,  dict(logger=logger, testa='testa'))])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.bind(port)
    http_server.start(2)
    tornado.ioloop.IOLoop.instance().start()
    # print('service finish')
#    app = tornado.web.Application(handlers=[(r"/searchgoods", SearchGoodsHandler,  dict(relaServer=serverRelaBaike, logger=logger))])
#    app = tornado.web.Application(handlers=[(r"/searchgoods", SearchGoodsHandler),] ,autoreload=False, debug=False)# by me
    
if __name__ == "__main__":
    
    argv=sys.argv[1:]
    (confPath,logPath)=para_set(argv)

    initPickle(confPath)
    print('start service')
    start(confPath,logPath)




    
    
    
    
    
    
    
    
    
    
    
    
