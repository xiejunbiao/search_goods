# -*- coding: utf-8 -*-
"""
Created on Mon May  4 10:35:06 2020

@author: lijiangman
"""

import os, time, random, json
# from whoosh import qparser, scoring
from whoosh.analysis import StandardAnalyzer
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema,TEXT,ID,NUMERIC, FieldType
# from jieba.analyse import ChineseAnalyzer
# from whoosh.analysis import NgramTokenizer
from whoosh.query import Every, Term
from whoosh.writing import AsyncWriter

# from searchmain.inition_db import smd
from searchhotmain.inition_db import SelectMysqlDatabase
# from searchhotmain.inition_config import config_result##数据库配置
# from searchhotmain.utils.log_con import getLogger
from searchmatch.loggerbyme import logger_search

import shutil
# from glob import *
"""
热搜算法，实例化一次，在这里面初始化一次，多简洁！全局变量（类似我写的语音助手）
"""

"""
创建商品字段的反向索引
品牌联想，海：海飞丝
简称联想，草：草莓
类别联想，水：水果
"""
"""
有的时候被占用，没有权限
ix=create_in(path_index, self.schema)
f = StructFile(open(self._fpath(name), "rb"), name=name, **kwargs)
PermissionError: [Errno 13] Permission denied: 'D:\\hisense\\projects\\searchassociation\\searchmain\\index_hot_all_filter\\index_hot_-1\\_MAIN_1.toc'

多个服务如果同时create或者open一个文件，在close之前，会加锁，导致报错，怎么办

https://www.osgeo.cn/whoosh/threads.html?highlight=lockerror
读是并发的，多线程的，但是写，有写锁

首先，最简单的，串行create，不要同时create索引。减少create的场景
或者try异常 ，retry重试机制

class whoosh.writing.AsyncWriter(index, delay=0.25, writerargs=None)
编写器对象的便利包装器，可能由于锁定而失败（即 filedb writer）此对象将尝试一次获取底层编写器，如果成功，则只将方法调用传递给它。
如果这个对象 can't 马上找个writer，他会的 buffer 删除、添加和更新内存中的方法调用，直到调用 commit() . 此时，这个对象将开始在一个单独的线程中运行，尝试一遍又一遍地获取编写器，一旦获得它，就“重放”它上面所有的缓冲方法调用。
在一个典型的场景中，作为Web事务的结果，您将向索引添加一个或几个文档，这样您就可以创建写入器、添加和提交，而不必担心索引锁、重试等。
例如，要找一个异步的writer，而不是这个：
>>> writer = myindex.writer()
这样做：
>>> from whoosh.writing import AsyncWriter
>>> writer = AsyncWriter(myindex)
参数
index -- 这个 whoosh.index.Index 写信给
delay -- 尝试实例化实际编写器之间的延迟（秒）。
writerargs -- 指定要传递到索引的关键字参数的可选字典 writer() 方法。
"""


class CreateSearchHotIndex:
    
    """
    根据搜索日志，建立热搜词反向索引
    """
    
    def __init__(self):

        """
        goods_short字段schema
        搜索联想：商品名和品牌字段，
        英文整体分词还是中文分词？
        
        商品名字段，麻辣香锅也要索引，用空格分开？yes，可以让商家上传关键词字段呢“麻辣香锅 麻辣”
        
        同一个索引文件夹下面，默认读取最新修改时间的？有时候自动创建了多个索引，重启spyder，又恢复成一个
        """

        """
        索引路径：
        
        还有一种简单的方案：
        第一次初始化，要create生成
        log统计索引
        置顶索引
        过滤索引
        然后再读取索引生成最终的热搜索引
        
        然后定时全量create：log统计+置顶+过滤（与初始化相同）
        
        然后：
        支持置顶更新
        支持过滤更新
        
        每次更新，只需生成create一个索引和读取所有索引，走一遍流程即可
        相当于，先生成若干索引，再读取全部索引进行组合，生成最终索引。
        提炼出三个更新模块：初始化更新模块、置顶更新模块、过滤更新模块
        """
               
        """
        param 
        """
        pathDir=os.path.dirname(__file__)
        self.path_index_all = os.path.join(pathDir, 'index_all')
        self.path_index_hot_log = os.path.join(pathDir, 'index_all', 'index_hot_log','index')
        self.path_index_hot_preset=os.path.join(pathDir, 'index_all', 'index_hot_preset','index')
        self.path_index_online = os.path.join(pathDir, 'index_all', 'index_online','index')
        self.path_index_hot_filter = os.path.join(pathDir, 'index_all', 'index_hot_filter','index')
        # """ 日志最小行数"""
        # line_num=50000
        # """ 搜索热搜关键词数目下限"""
        # query_number=200
        self.nSum=10
        # self.smd=SelectMysqlDatabase(config_result, line_num, query_number)
        self.smd=SelectMysqlDatabase()

        """
        小区编码不能放到这里，应该实时获取！！
        """
        # area_codes=list(self.smd.area_code_init())
        # area_codes += ['-1']##加入默认小区编码
        # # self.area_codes=['A2018012300015']
        # self.area_codes=area_codes
        # self.path_schema_dict=path_schema_dict        
        """
        param 
        """
        # self.path_index_hot=path_index_hot
        # self.path_index_hot_preset=path_index_hot_preset
        # self.path_index_hot_filter=path_index_hot_filter
        # self.path_index_online=path_index_online
        """
        goods_short字段schema
        搜索联想：商品名和品牌字段，
        英文整体分词还是中文分词？
        
        商品名字段，麻辣香锅也要索引，用空格分开？yes，可以让商家上传关键词字段呢“麻辣香锅 麻辣”
        
        同一个索引文件夹下面，默认读取最新修改时间的？有时候自动创建了多个索引，重启spyder，又恢复成一个
        """
        self.schema_online=Schema(goods_short=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                                  goods_brand=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                                  shopCode=ID(stored=True, unique=False),  # ID不用分词
                                  spu_code=ID(stored=True, unique=True))  # spu_code是唯一值
                           # area_code = ID(stored=True, unique=False),##ID不用分词
                           # search_frequency = NUMERIC(numtype=int, stored=True, sortable=True))##搜索频次
        # 为了可以针对某字段排序，在定义schema时，需对该字段添加sortable=True参数.
        # 也可以针对没定义sortable=True的字段排序，但是比较没有效率。

        """
        不用area_code，应该分小区建立索引！否则数据量太大
        
        search_frequency = NUMERIC(numtype=int, stored=True, sortable=True))
        数值类型设置为float，添加add小数到writer报错的原因找到啦，是因为设置了sortable=True参数。
    
        """
        self.schema=Schema(goods_short = TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                           goods_brand = TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                           spu_code = ID(stored=True, unique=True),##spu_code是唯一值，用来update索引
                           search_frequency = NUMERIC(numtype=int, stored=True, sortable=True))##搜索频次
        # 为了可以针对某字段排序，在定义schema时，需对该字段添加sortable=True参数.
        # 也可以针对没定义sortable=True的字段排序，但是比较没有效率。

        # 店铺暂时不支持运营后台配置置顶？
        self.schema_log=Schema(goods_short = TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                               shopCode = ID(stored=True, unique=False),##ID不用分词
                               search_frequency = NUMERIC(numtype=int, stored=True, sortable=True))##搜索频次


        self.schema_filter=Schema(goods_short=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                           goods_brand = TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                           spu_code = ID(stored=True, unique=True),##spu_code是唯一值，用来update索引
                           shopCode = ID(stored=True, unique=False),##ID不用分词
                           search_frequency = NUMERIC(numtype=int, stored=True, sortable=True))##搜索频次


    def get_area_codes(self):

        ac, sc=self.smd.area_code_init()
        area_codes=list(ac)
        area_codes += ['-1']##加入默认小区编码
        shop_codes=list(sc)
        shop_codes += ['-1']##加入默认店铺编码

        return area_codes, shop_codes

    """
    原始热搜日志索引
    """
    
    def create_index_hot_log(self, area_code, result_area):
        """
        嵌套子程序
        """
        path_index=self.path_index_hot_log+"_"+area_code
        if not os.path.exists(path_index):
            os.makedirs(path_index)
        while True:
            try:
                ix=create_in(path_index, self.schema_log)##运行两遍之后，又出现了之前的错误！PermissionError: [Errno 13] Permission denied
                break
            except Exception as e:
                print(e)
                time.sleep(2)##second
        # writer=ix.writer()
        writer = AsyncWriter(ix)
        for shopCode, vDict in result_area.items():
            for i, v in vDict.items():
                writer.add_document(goods_short=i, search_frequency=v, shopCode=shopCode)
        writer.commit()  # 自动关闭

    def create_index_hot_preset(self, area_code):

        hot_search_words = self.smd.query_for_hotsearch_words(area_code)
        """
        暂时屏蔽mysql的预置词
        """
        # hot_search_words=[""]
        spuDict = {"areaCode": area_code, "hotPreset": hot_search_words}
        self.update_hot_preset(spuDict)

    def update_hot_preset(self, spuDict):

        """
        运营后台预置商品，增加、删除索引，生成预置词索引
        """
        area_code=spuDict["areaCode"]
        goods_short_list=spuDict["hotPreset"]
        path_index=self.path_index_hot_preset+"_"+area_code
        # ix_hot=open_dir(index_hot)
        if not os.path.exists(path_index):
            os.makedirs(path_index)
        # ix=create_in(path_index, self.schema)
        while True:
            try:
                ix=create_in(path_index, self.schema)##运行两遍之后，又出现了之前的错误！PermissionError: [Errno 13] Permission denied       
                break
    
            except Exception as e:
                # self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
                # print(" hehe [Errno 13] Permission denied ")
                print(e)
                time.sleep(2)##second
        
        # writer=ix.writer()
        writer = AsyncWriter(ix)

        sf=100000
        spu_code_int=0
        for i in goods_short_list:
            # print(i)
            spu_code_int += 1
            if i=='':
                # search_frequency=0
                continue
            else:
                search_frequency=sf-spu_code_int
            """
            按照spu_code唯一索引进行更新
            """
            writer.update_document(spu_code=str(spu_code_int), goods_short=i,
                                   search_frequency=search_frequency)
        writer.commit(optimize=True)
        """
        # Merge small segments but leave large segments, trying to
        # balance fast commits with fast searching:
        writer.commit()

        # Merge all segments into a single segment:
        writer.commit(optimize=True) 
        """                                
        
    def create_index_hot_online(self, area_code):

        schema_list=self.smd.get_goods_schema(area_code)
        path_index=self.path_index_online+"_"+area_code
        if not os.path.exists(path_index):
            os.makedirs(path_index)
        # ix=create_in(path_index, self.schema)
        while True:
            try:
                ix=create_in(path_index, self.schema_online)##运行两遍之后，又出现了之前的错误！PermissionError: [Errno 13] Permission denied       
                break
            except Exception as e:
                print(e)
                time.sleep(2)  # second
        # writer=ix.writer()
        writer = AsyncWriter(ix)
        for i in schema_list:
            # print('goods_short', i)
            writer.add_document(goods_short=i['goods_short'],
                                goods_brand=i['goods_brand'],
                                shopCode=i['shop_code'],
                                spu_code=i['spu_code'])  # 增加商品上下架的服务。修改一个大bug，就是在线商品简称索引，忘了add添加spu_code

        writer.commit()  # 自动关闭

    def create_index_online_filter(self, area_code, results_hot_filter):
        """
        生成最终的热搜词索引
        """
        path_index=self.path_index_hot_filter+"_"+area_code
        if not os.path.exists(path_index):
            os.makedirs(path_index)
        # ix=create_in(path_index, self.schema)##运行两遍之后，又出现了之前的错误！PermissionError: [Errno 13] Permission denied
        # ix.close
        while True:
            try:
                ix=create_in(path_index, self.schema_filter)##运行两遍之后，又出现了之前的错误！PermissionError: [Errno 13] Permission denied
                break
            except Exception as e:
                # self.__logger.info(traceback.format_exc()) #返回异常信息的字符串，可以用来把信息记录到log里;
                # print(" hehe [Errno 13] Permission denied ")
                print(e)
                time.sleep(2)##second
        # writer=ix.writer()
        writer = AsyncWriter(ix)

        for shopCode, results in results_hot_filter.items():

            result_num=len(results)
            for i in range(result_num):
                """
                这样list(sc_hot.lexicon("goods_short"))，顺序不对
                """
                # writer.add_document(goods_short=results_hot_filter[i], search_frequency=(result_num-i)/10.0)
                """
                不能用小数，会自动转为整数
                """
                result_str = json.dumps(results[i], ensure_ascii=False)
                writer.add_document(goods_short=result_str, search_frequency=(result_num-i), shopCode=shopCode)
        writer.commit()  # 自动关闭

    def update_index_online_filter(self, area_code, results_hot_filter):
        path_index=self.path_index_hot_filter+"_"+area_code
        if os.path.exists(path_index):
            ix = open_dir(path_index)
            writer = AsyncWriter(ix)
            # query = Term("shopCode", "-1")
            # writer.delete_by_query(query)

            for shopCode, results in results_hot_filter.items():
                query = Term("shopCode", shopCode)
                writer.delete_by_query(query)

                result_num = len(results)
                for i in range(result_num):
                    """
                    这样list(sc_hot.lexicon("goods_short"))，顺序不对
                    """
                    # writer.add_document(goods_short=results_hot_filter[i], search_frequency=(result_num-i)/10.0)
                    """
                    不能用小数，会自动转为整数
                    """
                    result_str = json.dumps(results[i], ensure_ascii=False)
                    writer.add_document(goods_short=result_str, search_frequency=(result_num - i), shopCode=shopCode)

            writer.commit()  # 自动关闭

    def create_index_hot_filter(self, area_code, shopCodes):
        """
        读取三个索引，生成最终过滤索引
        """
        def get_result_filter_online(results_hot_list_qc, results_online_list, results_hot_preset):
            results_hot_filter = []
            n = 0
            for i in results_hot_list_qc:
                if i in results_online_list or i in results_hot_preset:
                    n += 1
                    if n > self.nSum:
                        break
                    else:
                        # results_hot_filter.append(i)
                        # 把list元素改为dict，区分是否是预置词
                        if i in results_hot_preset:
                            results_hot_filter.append({"word": i, "hot": True})
                        else:
                            results_hot_filter.append({"word": i, "hot": False})

            result_num = len(results_hot_filter)

            if result_num < self.nSum:
                # results_hot_filter=results_hot_list[:self.nSum]
                """
                从在线商品中补充，去重
                注意：需要先置顶，再过滤在线
                """
                results_hot_filter_list = [i["word"] for i in results_hot_filter]
                aa = results_hot_filter_list + results_online_list  # 置顶
                aa_qc = list(set(aa))
                aa_qc.sort(key=aa.index)
                aa_qc_online = []
                n = 0
                for i in aa_qc:
                    if i in results_online_list or i in results_hot_preset:
                        n += 1
                        if n > self.nSum:
                            break
                        else:
                            # aa_qc_online.append(i)
                            if i in results_hot_preset:
                                aa_qc_online.append({"word": i, "hot": True})
                            else:
                                aa_qc_online.append({"word": i, "hot": False})
                # print('less than %s' % (self.nSum))
                results_hot_filter = aa_qc_online
            return results_hot_filter

        def filter_by_online_shop(shopCode):

            """
            预置热搜词置顶---目前只支持某小区的所有店铺，不支持某店铺预置热搜词--mysql表格需要处理
            """
            if shopCode != "-1":
                results_hot_preset = []
            else:
                path_hot_preset = self.path_index_hot_preset + "_" + area_code
                if not os.path.exists(path_hot_preset):
                    results_hot_preset = []
                else:
                    results_hot_preset = open_index_hot_preset(path_hot_preset)
            """
            必须判断是否存在index_hot
            """
            path_hot_log = self.path_index_hot_log + "_" + area_code
            if not os.path.exists(path_hot_log):
                results_hot_log = []
            else:
                results_hot_log = open_index_hot_log(path_hot_log, shopCode)

            results_hot_list = results_hot_preset + results_hot_log
            # print(results_hot_preset)
            """
            results_hot_list 去重
            """
            results_hot_list_qc = list(set(results_hot_list))
            results_hot_list_qc.sort(key=results_hot_list.index)
            """
            在线过滤
            """
            path_index_online = self.path_index_online + "_" + area_code
            if not os.path.exists(path_index_online):
                # print("path_index_online not exsit", path_index_online)
                # results_hot_filter=[]
                # return results_hot_filter  还有置顶的呢，不能return
                results_online_list=[]
            else:
                results_online_list = open_index_online(path_index_online, shopCode)
            # print(results_online_list)
            results_hot_filter=get_result_filter_online(results_hot_list_qc, results_online_list, results_hot_preset)
            # print(results_hot_filter)
            return results_hot_filter

        def filter_by_online():

            rhf={}
            for shopCode in shopCodes:
                rhf[shopCode]=filter_by_online_shop(shopCode)
            # self.results_hot_filter=rhf
            return rhf

        def open_index_hot_preset(path_index):

            sc_hot = open_dir(path_index).searcher()
            # results_hot_list=list(sc_hot.lexicon("goods_short"))
            myquery = Every()
            results_hot = sc_hot.search(myquery, limit=None, sortedby='search_frequency', reverse=True)
            results_hot_list = [i['goods_short'] for i in results_hot]
            sc_hot.close()
            return results_hot_list

        def open_index_hot_log(path_index, shopCode):

            sc_hot=open_dir(path_index).searcher()
            # results_hot_list=list(sc_hot.lexicon("goods_short"))
            myquery = Every()
            filter = Term("shopCode", shopCode)
            results_hot=sc_hot.search(myquery, limit=None, filter=filter, sortedby='search_frequency', reverse=True)
            results_hot_list=[i['goods_short'] for i in results_hot]
            sc_hot.close()
            return results_hot_list
        
        def open_index_online(path_index, shopCode):

            ix_online=open_dir(path_index)
            # 搜索查询所有goods_short字段的数据
            sc_online=ix_online.searcher()
            # results_online_list=list(sc_online.lexicon("goods_short"))##https://www.osgeo.cn/whoosh/searching.html官网
            # results_online_list=[str(i, encoding = "utf-8") for i in sc_online.lexicon("goods_short")]
            myquery = Every()
            # 构造shop_code="-1"的情况
            if shopCode=="-1":
                filter=None
            else:
                filter = Term("shopCode", shopCode)
            results_hot=sc_online.search(myquery, limit=None, filter=filter)
            results_online_list=[i['goods_short'] for i in results_hot]
            sc_online.close()
            return results_online_list

        # """
        # 预置热搜词置顶---目前只支持某小区的所有店铺，不支持某店铺预置热搜词
        # """
        # path_hot_preset=self.path_index_hot_preset+"_"+area_code
        # if not os.path.exists(path_hot_preset):
        #     results_hot_preset = []
        # else:
        #     results_hot_preset = open_index_hot(path_hot_preset)

        # results_hot_list=results_hot_list_preset+results_hot_list_log
        # """
        # results_hot_list 去重
        # """
        # results_hot_list_qc=list(set(results_hot_list))
        # results_hot_list_qc.sort(key=results_hot_list.index)
        # """
        # 在线过滤
        # """
        # path_index_online=self.path_index_online+"_"+area_code
        # if not os.path.exists(path_index_online):
        #     print("path_index_online not exsit", path_index_online)
        #     return
        # else:
        #     results_online_list=open_index_online(path_index_online)
        # 不过滤置顶热搜词results_hot_list
        # results_hot_filter=filter_by_online_index(results_hot_list_qc, results_online_list)
        results_hot_filter=filter_by_online()

        return results_hot_filter

    def init_and_update_index_all(self):
        """
        初始化和定时更新所有索引
        """
        # 先全量删除文件夹index_all
        if os.path.exists(self.path_index_all):
            shutil.rmtree(self.path_index_all)  # 递归删除一个目录以及目录内的所有内容

        area_codes, shopCodes=self.get_area_codes()
        result_seris=self.smd.get_schema_list_schedule()

        for area_code in area_codes:
            if area_code == "-1":  # 暂时屏蔽area_code="-1"
                continue

            # if area_code != "A2018012300015":
            #     continue

            if area_code in result_seris:
                result_area=result_seris[area_code]
                # shopCodes = result_area.keys()
                self.create_index_hot_log(area_code, result_area)
            self.create_index_hot_preset(area_code)
            self.create_index_hot_online(area_code)
            results_hot_filter=self.create_index_hot_filter(area_code, shopCodes)
            # 新建索引
            self.create_index_online_filter(area_code, results_hot_filter)

    def update_index_preset(self, spuDict):
        """
        更新预置索引
        """
        # pass##暂时屏蔽
        # area_code=spuDict["areaCode"]
        # self.create_index_hot_preset(spuDict)
        # 注意：在线词等其他的create索引的时候注意，会清空原来的索引。

        self.update_hot_preset(spuDict)
        # self.create_index_hot_filter(spuDict["areaCode"], ["-1"])  # 目前只适用所有店铺-1---而且清空了索引
        results_hot_filter = self.create_index_hot_filter(spuDict["areaCode"], ["-1"])# 目前只适用所有店铺-1---而且更新了索引
        self.update_index_online_filter(spuDict["areaCode"], results_hot_filter)

    """
    测试
    """
    def search_hot_filter_test_all(self, arg_dict):
        
        # area_codes=list(self.smd.area_codes)
        # area_codes += ['-1']##加入默认小区编码
        # area_codes=['A2018012300015','-1']
        # for i in area_codes:
        results_hot_list=self.search_hot_filter_test(arg_dict)
        print(results_hot_list)
        # logger.info(results_hot_list)

    def search_hot(self, arg_dict):

        def vers1():
            if arg_dict['shopCode'] == '-1':
                # start = time.time()
                index_hot_filter = self.path_index_hot_filter + "_" + arg_dict['areaCode']
                if os.path.exists(index_hot_filter):
                    sc_hot = open_dir(index_hot_filter).searcher()
                    # results_hot_list=list(sc_hot.lexicon("goods_short"))##好像是默认字母顺序
                    # results_hot_list=[str(b, encoding = "utf-8") for b in results_hot_list]
                    myquery = Every()
                    filter = Term("shopCode", arg_dict['shopCode'])
                    results_hot = sc_hot.search(myquery, limit=None, filter=filter, sortedby='search_frequency',
                                                reverse=True)  # limit=None返回所有结果
                    results_hot_list = [json.loads(i['goods_short']) for i in results_hot]
                    # results_hot_list=[i['search_frequency'] for i in results_hot]
                    # elapsed = time.time() - start
                    # print("Time used:%s s" % (elapsed))  # 时间长？？60ms
                    sc_hot.close()  # 记得close啊sc_hot。close，而且要判断，是否存在dir，不存在，则返回空list
                else:
                    # results_hot_list=['']*10
                    results_hot_list = []
            else:
                results_hot_list=cshi.smd.get_shop_hot_goods(arg_dict)

            if len(results_hot_list) == 0:
                results_hot_list = ""

            return results_hot_list

        if arg_dict['versionCode']=="-1":
            tep_list=vers1()
            results_hot_list=[i["word"] for i in tep_list]
        else:
            results_hot_list=vers1()

        return results_hot_list

    def search_hot_filter_test(self, arg_dict):
        """
        搜索热搜测试
        """
        results_hot_list=self.search_hot(arg_dict)
        page_dict={
                    "resultCode": "0",
                    "msg": "成功",
                    "data":results_hot_list
            }
        result_json=json.dumps(page_dict, ensure_ascii=False)##False解决中文乱码
    
        return result_json

    def search_hot_filter_intro_test(self, arg_dict):
        
        """
        搜索引导词测试
        """
        # start = time.time()
        # # pathDir=os.path.dirname(__file__)
        # # path_index_hot_filter = os.path.join(pathDir, 'index_hot_all_filter','index_hot')
        # index_hot_filter=self.path_index_hot_filter+"_"+area_code
        # if os.path.exists(index_hot_filter):
        #     sc_hot=open_dir(index_hot_filter).searcher()
        #     # results_hot_list=list(sc_hot.lexicon("goods_short"))##好像是默认字母顺序
        #     # results_hot_list=[str(b, encoding = "utf-8") for b in results_hot_list]
        #     myquery = Every()
        #     results_hot=sc_hot.search(myquery, limit=None, sortedby='search_frequency', reverse=True)#limit=None返回所有结果
        #     results_hot_list=[json.loads(i['goods_short']) for i in results_hot]
        #     # results_hot_list=[i['search_frequency'] for i in results_hot]
        #     elapsed = time.time() - start
        #     print("Time used:%s s" %(elapsed))    # 时间长？？60ms
        #     sc_hot.close()## 记得close啊sc_hot。close，而且要判断，是否存在dir，不存在，则返回空list
        #
        # else:
        #     # results_hot_list=['']*10
        #     results_hot_list=[]

        rhl=self.search_hot(arg_dict)

        if arg_dict['versionCode']=="-1":
            results_hot_list=rhl
        else:
            results_hot_list=[i["word"] for i in rhl]

        if len(results_hot_list)>0:
            results_hot_intro=random.choice(results_hot_list)
        else:
            results_hot_intro=""
        page_dict={
                    "resultCode": "0",
                    "msg": "成功",
                    "data":results_hot_intro
            }
        result_json=json.dumps(page_dict, ensure_ascii=False)##False解决中文乱码
    
        return result_json


"""
初始化和定时任务统计热搜词
"""

    
def init_hot_search_test():

    # 初始化阶段：
    # 首先，创建在线过滤索引index1
    # 然后，创建原始热搜索引index2
    # 然后，创建过滤热搜索引index3，逻辑是：原始热搜index2，再用index1过滤
    """
    首先，建立在线过滤索引
    因为要初始化和定时任务统计热搜，所以必须实时读取小区编码，而非是固定的值
    定时任务初始化：搜索词，统计，保存到本地pkl
    预置词置顶+热搜+在线过滤，得到最终的热搜过滤索引
    第一次运行，然后再定时运行？？？
    """
    # （备注：在测试环境下，为了使热搜词的统计效果较好，以及达到日志数量下限，需要将线上搜索日志文件stdout.log替换掉测试环境的搜索日志log）
    # 如果达不到日志下限，则不会建立索引文件夹
    logger_search.info("init_hot_search_test")
    cshi.init_and_update_index_all()
    arg_dict={'areaCode': 'A2018012300015', 'shopCode': '-1', 'versionCode': '1'}
    cshi.search_hot_filter_test(arg_dict)
    # arg_dict={'areaCode': 'A2018012300015', 'shopCode': '356396279627776000'}
    # arg_dict = {'areaCode': 'A2018012300015', 'shopCode': '342549182956699648'}
    # cshi.search_hot_filter_test_all(arg_dict)
    logger_search.info("finish_hot_search_test")

    
# """
# 建立运营后台预置热搜词索引
# """
# goods_short_list=['运营预置商品4','运营预置商品2','运营预置商品3']
# # goods_short_list=['','','','']##空的情况下，词频置零
# area_code='-1'
# si=CreateSearchHotPresetIndex()
# si.creatIndex(area_code=area_code, goods_short_list=goods_short_list)

# """
# 热搜算法，实例化一次，在这里面初始化一次，多简洁！
# """


def search_online_test():
    path_index=cshi.path_index_online+"_A2018012300015"
    # path_index=cshi.path_index_online+"_-1"
    # path_index=cshi.path_index_hot_preset+"_A2018012300015"
    ix_online=open_dir(path_index)
    # 搜索查询所有goods_short字段的数据
    sc_online=ix_online.searcher()
    
    # qp=qparser.MultifieldParser(fieldnames=["goods_short"], schema=cshi.schema_online)
    # myquery=qp.parse("榴莲")
    myquery=Every()
    print('seg:',myquery)
    results=sc_online.search(myquery, limit=None)#limit=None返回所有结果

    if len(results)>0:
        for hit in results:
            print('hit:\n', hit)
            # print('content:\n', hit['content'])
            print('score:\n', hit.score)
    else:
        print('empty')

    # results_online_list=[str(i, encoding = "utf-8") for i in sc_online.lexicon("goods_short")]
    sc_online.close()

    return results


def update_hot_search_test():

    """
    在__main__这里是全局变量，能被class里面的变量引用，因此放到一个函数里面
    goods_short_list竟然可以被类里面的变量调用！！！！
    """
    """
    更新索引的设计：
    我们最终的热搜词，要经过三个流程，热搜词统计，置顶，过滤
    热搜索引
    预置+热搜索引
    预置+热搜+过滤索引
    根据不同的更新，去更新相应的索引
    如果log日志变化，定时更新的时候，要经过所有1、2、3个流程
    如果置顶词变化，则经过2、3流程
    如果过滤词发生变化，则只经过3流程
    
    代码设计要根据上述，清晰地展示出上述思路！
    
    还有一种简单的方案：
    第一次初始化，要create生成
    log统计索引
    置顶索引
    过滤索引
    然后再读取索引生成最终的热搜索引
    
    然后定时全量create：log统计+置顶+过滤（与初始化相同）
    
    然后：
    支持置顶更新
    支持过滤更新
    
    每次更新，只需生成create一个索引和读取所有索引，走一遍流程即可
    相当于，先生成若干索引，再读取全部索引进行组合，生成最终索引。
    提炼出三个更新模块：初始化更新模块、置顶更新模块、过滤更新模块
    """
    print("-------update_hot_search_test")
    
    # area_codes=['A2018012300015', '-1']
    # cshi.create_search_words_init(area_codes)
    arg_dict={'areaCode': 'A2018012300015', 'shopCode': '-1', 'versionCode': '1'}
    cshi.search_hot_filter_test(arg_dict)
    """
    更新建立运营后台预置热搜词索引
    """
    print("----preset words")
    # goods_short_list=['鸡蛋', '机器猫梨', '呵呵呵你好帅', '好累啊']
    goods_short_list=['鸡蛋hyt', '机器猫梨']

    areaCode='A2018012300015'
    spuDict={"areaCode":areaCode,"hotPreset":goods_short_list}
    # si=UpdateSearchHotIndex()
    # ushi.update_hot_preset_index(spuDict)
    cshi.update_index_preset(spuDict)

    # goods_short_list=['','','','']##空的情况下，词频置零
    goods_short_list=['鸡蛋', '机器猫', '苹果', '洗发水', '洗发露', '牛奶', '梨']
    areaCode='-1'
    spuDict={"areaCode":areaCode,"hotPreset":goods_short_list}
    cshi.update_index_preset(spuDict)
    cshi.search_hot_filter_test(arg_dict)

cshi=CreateSearchHotIndex()


if __name__=='__main__':
    """
    这里是全局变量，能被class里面的变量引用，因此放到一个函数里面
    """
    # 同步数据之后，验证online数据库
    # search_online_test()
    # init_hot_search_test()
    # search_online_test()
    # update_hot_search_test()


    start = time.time()
    arg_dict={'areaCode': 'A2018012300015', 'shopCode': '356396279627776000', 'versionCode': '1'}
    result_json=cshi.search_hot_filter_test(arg_dict)
    print(result_json)
    elapsed = time.time() - start
    print("Time used:%s s" %(elapsed))    # 时间长？？60ms

    # schema = Schema(goods_short=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)))
    #
    # pathDir = os.path.dirname(__file__)
    # path_index_all = os.path.join(pathDir, 'abcsss')
    # # if not os.path.exists(path_index_all):
    # #     os.makedirs(path_index_all)
    # ix = create_in(path_index_all, schema)  ##运行两遍之后，又出现了之前的错误！PermissionError: [Errno 13] Permission denied
    # # ix = open_dir(path_index_all)
    # writer = AsyncWriter(ix)
    # writer.add_document(goods_short="你好测试")
    # writer.commit()  # 自动关闭
    # print('ok')
    # 删除toc文件或者重新create
    # ix = create_in(path_index_all, schema)
    # file=os.path.join(path_index_all, '_MAIN_1.toc')

    # 在index文件中修改源码，复现了错误
    # def matchWildcard(rootPath="", pattern=""):
    #     rootPath = os.path.abspath(rootPath)
    #     results = []
    #     for root, dirs, files in os.walk(rootPath):
    #         for match in glob(os.path.join(root, pattern)):
    #             results.append(match)
    #     return results
    # results=matchWildcard(path_index_all, "*toc*")
    # print(results)
    # file=os.path.exists(results[0])
    # print(file)
    # os.remove(results[0])
    # if os.path.isfile(file):
    #     os.remove(file)
    #     print(os.path.exists(file))
    # else:
    #     print(os.path.isfile(file))
    # os.remove(results[0])