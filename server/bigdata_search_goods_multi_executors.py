# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 15:01:02 2019

@author: lijiangman
"""
# 更改日志配置--不适用于多进程，修改日志方案

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
from searchmatch.search_for_xwj import search_main, search_query_revise  # 模块名字不能有小数点

from searchhotmain.index_create import cshi
from index_update_mq.goods_data_update import update_index_online

from searchmatch.loggerbyme import logger_search
from searchmatch.sensor_log import search_event_log

from searches.esclient_byme import index_search_sense, create_index_sense, index_sense


class SearchGoodsHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger):
        self.__logger = logger

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        query = self.get_argument('searchKey')
        pageNum = self.get_argument('page')
        rows = self.get_argument('rows')  # 每页有多少行
        ownerCode = self.get_argument('ownerCode', default='ABCDEF1234567_Default')
        areaCode=self.get_argument('areaCode')
        shopCode=self.get_argument('shopCode', default='-1')  # 改成默认--好像在浏览器中必须输入数字，不能输入字符--redis缓存做key的时候，改
        sortMethod=self.get_argument('sortMethod', default='-1')  # 非必须--默认是-1  改为8888ljm也行
        searchDim = self.get_argument('searchDim', default='1')  # 默认是搜索维度

        # 由于热搜词要统计“areaCode”，所以这里还是用原来的驼峰式
        arg_dict = {'ownerCode':ownerCode, 'query': query, 'areaCode': areaCode, 'sort_method': sortMethod, 'search_dim': searchDim, 'rows': rows, 'page': pageNum, 'shop_code': shopCode}
        page_json=yield self.get_search_main(arg_dict)
        self.write(page_json)

    @run_on_executor
    def get_search_main(self, arg_dict):
        try:
            # input_dict={'searchKey':text_input,'page':pageNum,'rows':rows,'areaCode':areaCode,'shopCode':shopCode,'sortMethod':sortMethod}
            input_json=json.dumps(arg_dict, ensure_ascii=False)  # False解决中文乱码
            """
            由于之前的日志格式用input_dict-作为分隔符，需要热搜词统计，所以这里暂且不更改名字了
            """
            self.__logger.info("input_dict-"+input_json)
            page_dict=search_main(arg_dict)
            page_json = json.dumps(page_dict, ensure_ascii=False)  # False解决中文乱码

            if arg_dict['search_dim']=='1':
                # search_event_log(arg_dict, page_json)同步执行
                # self.executor.submit(search_event_log(), self.config_dict, self.__logger)同步执行
                self.executor.submit(search_event_log, arg_dict, page_dict)  # 异步执行

            # key事件名 用户唯一id 属性。。。。。
            self.__logger.info("search_return-"+page_json)
            # tornado异步写日志，模仿神策，把事件放入一个json里面 self.executor.submit(self.__logger.info, search_event_json)  # 异步执行
            search_event_json={"event_name": "SearchRequest", "input_arg": arg_dict, "output_arg": page_dict}
            self.executor.submit(self.__logger.info, search_event_json)  # 异步执行
        except Exception as e:
            self.__logger.info("error:")  # 用input_dict-作为分隔符
            self.__logger.info(e)  # 用input_dict-作为分隔符
            # self.__logger.info("traceback My:")##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc())  # 返回异常信息的字符串，可以用来把信息记录到log里;
            page_json={}

        return page_json


class SearchMinganHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger):
        self.__logger = logger

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        query = self.get_argument('searchKey')
        # arg_dict = {'query': query}
        page_json = yield self.get_search_main(query)
        self.write(page_json)

    @tornado.gen.coroutine
    def post(self):

        args = json.loads(self.request.body)
        # spuDict = {key: value for key, value in args.items()}
        query = args['searchKey']
        page_json = yield self.get_search_main(query)

        self.write(page_json)

    @run_on_executor
    def get_search_main(self, query):

        try:
            query_res = index_search_sense(query)
            # self.__logger.info("revise_input-" + input_json)  ##用input_dict-作为分隔符
            # self.__logger.info("revise-return-" + query_revise)
            search_event_json = {"event_name": "MinganSearch", "query": query, "queryRes": query_res}
            self.executor.submit(self.__logger.info, search_event_json)  # 异步执行

        except Exception as e:
            self.__logger.info("error:")  ##用input_dict-作为分隔符
            self.__logger.info(e)  ##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc())  # 返回异常信息的字符串，可以用来把信息记录到log里;

            data_dict = {
                "queryResult": "0",
                "queryRes": query
            }
            query_res = {
                "resultCode": "0",
                "msg": "操作成功",
                "data": data_dict
                #               'testSpu':spu_code_search##测试分页的正确性
            }
            query_res = json.dumps(query_res, ensure_ascii=False)

        return query_res


class SearchReviseHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    def initialize(self, logger, testa):
        self.__logger = logger
        self.testa=testa

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        query = self.get_argument('searchKey')
        arg_dict={'query': query}
        # arg_dict['query']=query
        # arg_dict['page']=pageNum
        # arg_dict['rows']=rows
        # arg_dict['areaCode']=areaCode
        # arg_dict['shopCode']=shopCode
        # arg_dict['sortMethod']=sortMethod
        # 必须加self，否则找不到此函数
        page_json=yield self.get_search_main(arg_dict)
        self.write(page_json)

    @run_on_executor
    def get_search_main(self, arg_dict):
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
        
            self.__logger.info("revise_input-"+input_json)##用input_dict-作为分隔符
            self.__logger.info("revise-return-"+query_revise)

        except Exception as e:
            self.__logger.info("error:")##用input_dict-作为分隔符
            self.__logger.info(e)##用input_dict-作为分隔符
            # self.__logger.info("traceback My:")##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;

        return query_revise


class SearchHotHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)
    def initialize(self, logger):
        self.__logger = logger

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        areaCode = self.get_argument('areaCode')
        shopCode = self.get_argument('shopCode', default='-1')
        versionCode = self.get_argument('version', default='-1')  # 非必须--默认是-1，因为返回格式发生了变化，所以要根据用户版本返回不同的格式。兼容两个版本
        arg_dict={'areaCode': areaCode, 'shopCode': shopCode, 'versionCode': versionCode}
        # arg_dict['areaCode']=areaCode

        # 'version':"-1"默认
        # 'version': "1"新版本
        # 'shopCode':"-1" 默认店铺编码

        page_json=yield self.get_search_main(arg_dict)
        self.write(page_json)

    @run_on_executor
    def get_search_main(self, arg_dict):

        try:
            result_json=cshi.search_hot_filter_test(arg_dict)
            
            # input_dict={'searchKey':text_input,'page':pageNum,'rows':rows,'areaCode':areaCode,'shopCode':shopCode,'sortMethod':sortMethod}
            input_json=json.dumps(arg_dict, ensure_ascii=False)##False解决中文乱码
        
            self.__logger.info("search_hot_input-"+input_json)##用input_dict-作为分隔符
            self.__logger.info("search_hot_return-"+result_json)

        except Exception as e:
            self.__logger.info("error:")##用input_dict-作为分隔符
            self.__logger.info(e)##用input_dict-作为分隔符
            # self.__logger.info("traceback My:")##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
            data=[]
            result_json={"resultCode":"1","msg":"失败","data":data}

        return result_json


class SearchHotIntroHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)
    def initialize(self, logger):##删除一个输入参数
        self.__logger = logger

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        areaCode = self.get_argument('areaCode')
        fromType = self.get_argument('fromType')
        shopCode = self.get_argument('shopCode', default='-1')
        versionCode = self.get_argument('version', default='-1')  # 非必须--默认是-1，因为返回格式发生了变化，所以要根据用户版本返回不同的格式。兼容两个版本
        arg_dict={'areaCode': areaCode, 'shopCode': shopCode, 'versionCode': versionCode, 'fromType': fromType}

        # arg_dict={}
        # arg_dict['areaCode']=areaCode
        # arg_dict['fromType']=fromType
       
        #必须加self，否则找不到此函数
        page_json=yield self.get_search_main(arg_dict)
        
        self.write(page_json)

    @run_on_executor
    def get_search_main(self, arg_dict):

        try:
            result_json=cshi.search_hot_filter_intro_test(arg_dict)
            
            # input_dict={'searchKey':text_input,'page':pageNum,'rows':rows,'areaCode':areaCode,'shopCode':shopCode,'sortMethod':sortMethod}
            input_json=json.dumps(arg_dict, ensure_ascii=False)##False解决中文乱码
        
            self.__logger.info("search_hotintro_input-"+input_json)##用input_dict-作为分隔符
            self.__logger.info("search_hotintro_return-"+result_json)

        except Exception as e:
            self.__logger.info("error:")##用input_dict-作为分隔符
            self.__logger.info(e)##用input_dict-作为分隔符
            # self.__logger.info("traceback My:")##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
            data=[]
            result_json={"resultCode":"1","msg":"失败","data":data}

        return result_json


class SearchHotPresetHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)
    def initialize(self, logger):##删除一个输入参数
        self.__logger = logger

    @tornado.gen.coroutine
    def get(self):
        """get请求"""
        pass

    @tornado.gen.coroutine
    def post(self):
        
        args = json.loads(self.request.body)
        spuDict={key:value for key,value in args.items()}
        page_json=yield self.get_rec_main(spuDict)
        
        self.write(page_json)
    
    @run_on_executor
    def get_search_main(self,pageNum,rows,areaCode,cateCode,sortMethod):
        
        pass

    @run_on_executor
    def get_rec_main(self, spuDict):
        
        # input_dict={'page':pageNum,'rows':rows,'areaCode':areaCode,'cateCode':cateCode,'sortMethod':sortMethod,'spuCodeList':spuCodeList}
        input_json=json.dumps(spuDict,ensure_ascii=False)##False解决中文乱码
        try:
            cshi.update_index_preset(spuDict)
            self.__logger.info("search_hotpreset_input-"+input_json)##用input_dict-作为分隔符
            # self.__logger.info("search_hotpreset_return-"+page_json)
            # page_json="ok\n"+input_json这样java解析json错误
            result_json={"resultCode":"0","msg":"成功","data":input_json}
        except Exception as e:
            self.__logger.info("error:search_hotpreset_input-"+input_json)##用input_dict-作为分隔符
            self.__logger.info(e)##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
            result_json={"resultCode":"1","msg":"失败","data":input_json}

        return result_json


class SearchHotUpdateHandler(tornado.web.RequestHandler):
    """
    在线状态消息队列更新update_index_online
    """
    executor = ThreadPoolExecutor(20)
    def initialize(self, logger):##删除一个输入参数
        self.__logger = logger

    @tornado.gen.coroutine
    def post(self):
        
        args = json.loads(self.request.body)
        spuDict={key:value for key,value in args.items()}
        page_json=yield self.get_rec_main(spuDict)
        
        self.write(page_json)

    @run_on_executor
    def get_rec_main(self, spuDict):
        
        input_json=json.dumps(spuDict,ensure_ascii=False)##False解决中文乱码
        try:
            # cshi.update_index_online(spuDict)

            self.__logger.info("start-search_update_online_input-"+input_json)##用input_dict-作为分隔符
            update_index_online(spuDict)
            self.__logger.info("finish-search_update_online_input")##用input_dict-作为分隔符
            # page_json="ok\n"+input_json这样java解析json错误
            result_json={"resultCode":"0","msg":"成功","data":input_json}

        except Exception as e:
            self.__logger.info("error:search_update_online_input-"+input_json)##用input_dict-作为分隔符
            self.__logger.info(e)##用input_dict-作为分隔符
            # self.__logger.info("traceback My:")##用input_dict-作为分隔符
            self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
            result_json={"resultCode": "1", "msg": "失败", "data": input_json}

        return result_json


def start():

    # 为了在同一个进程里面实例化es
    #     """
    #     初始化敏感词
    #     """
    logger_search.info("Creating an es index min_gan_ci...")
    create_index_sense()
    logger_search.info("Es indexing min_gan_ci documents...")
    index_sense()
    logger_search.info('Finish es index min_gan_ci')

    """
    web服务
    """
    logger_search.info('Start web service')

    # web tornado
    port=6601
    # logger = getLogger()
    # logger_revise = getLogger(g_log_prefix_revise)#---目前会同时写入两个log路径
    # 下一步，用Python自带的log，Python输出到屏幕    
    app = tornado.web.Application(handlers=[
        (r"/cloudbrain-search/search/searchForGoods", SearchGoodsHandler,  dict(logger=logger_search)),
        (r"/cloudbrain-search/search/searchForMinG", SearchMinganHandler, dict(logger=logger_search)),
        (r"/cloudbrain-search/search/searchQueryRevise", SearchReviseHandler,  dict(logger=logger_search, testa='testa')),
        (r"/cloudbrain-search/search/searchHot", SearchHotHandler,  dict(logger=logger_search)),
        (r"/cloudbrain-search/search/searchHotIntro", SearchHotIntroHandler,  dict(logger=logger_search)),
        (r"/cloudbrain-search/search/searchHotPreset", SearchHotPresetHandler,  dict(logger=logger_search)),
        (r"/cloudbrain-search/search/searchHotUpdate", SearchHotUpdateHandler,  dict(logger=logger_search))])
    http_server = tornado.httpserver.HTTPServer(app)
    # app.listen(port)  #单进程
    http_server.bind(port)
    # http_server.start(10)
    http_server.start(num_processes=1)  # By default, we run the server in this process and do not fork any additional child process.
    tornado.ioloop.IOLoop.instance().start()
    #  app = tornado.web.Application(handlers=[(r"/searchgoods", SearchGoodsHandler,  dict(relaServer=serverRelaBaike, logger=logger))])
    #  app = tornado.web.Application(handlers=[(r"/searchgoods", SearchGoodsHandler),] ,autoreload=False, debug=False)# by me
    #  http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=-1
    #  http://10.18.222.105:6601/cloudbrain-search/search/searchForMinG?searchKey=我们热爱习近平


if __name__ == "__main__":
    
    print('the main is not this path')