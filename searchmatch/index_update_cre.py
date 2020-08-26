# -*- coding: utf-8 -*-
"""
Created on Fri May 15 19:55:22 2020

@author: lijiangman
"""

from whoosh import qparser, scoring, sorting
from whoosh.analysis import StandardAnalyzer
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, NUMERIC, STORED
# from whoosh.analysis import NgramTokenizer
from whoosh.query import Every
from whoosh.writing import AsyncWriter

# import numpy as np
# import json
import time
import datetime
import os.path
import pymysql
import traceback
import pandas as pd
from searchhotmain.inition_config import config_result  # 数据库配置
import searchhotmain.utils.mysql_comm as mc
from searchmatch.synonym_lib import synonym_dict

from searchmatch.analyzerbyme import ChineseAnalyzer, ChineseAnalyzerMerge
from searchmatch.loggerbyme import logger_search
import shutil

"""
官网
https://www.osgeo.cn/whoosh/indexing.html?highlight=lockerror#finishing-adding-documents
"""

# 增量索引
# 在为文档集合编制索引时，通常需要两个代码路径：一个是从头开始为所有文档编制索引，另一个是只更新已更改的文档（将Web应用程序放在需要根据用户操作添加/更新文档的位置）。
# 当商品上架频繁的时候，定时批量更新？？？或者每次上架单个商品都应该主动更新？
# 从零开始索引所有内容非常容易。下面是一个简单的例子：
# mysql数据库也有更新时间，可以仿照此文件增量更新的做法
# 您好，这样的： 这种writer-reader架构，一般思路是在缓存更新阶段由writer来解决一致性复问题，
# 当数据库数据变化时，同步更新制redis并确保缓存更新成功。 作为完整性判断，可以不检查全部的属性，而对数据使用一个自增的版本号（或时间戳）来判断是否最新。
# 作为后置的检测，可以优化来降低扫描的代价，如只针对百最近一个时间周期内（如10min）数据库度中更新过的数据，这个集合应该比较小，去redis中进行检查的代价会比较低。
# created_time和updated_time分别是
# ES的mysql增量更新！！

# 让刘尊彦，在cb_goods_spu_search和cb_goods_scope中设置updated_time自动生成。
# 仿照whoosh，只需要把mysql的更新时间（相当于文件的最新修改时间）记录即可，增量更新。
# 定时增量。
"""
mysql转whoosh初始化全量更新和定时增量更新
"""


class IndexUpdate:

    def __init__(self):
    
        self.ip=config_result['ip']
        self.user=config_result['user']
        self.password=config_result['password']
        self.db=config_result['db']
        self.port=config_result['port']
        
        self.gs_tb=config_result['tb_goods_scope']  # 小区代码和spu代码对应关系
        self.gss_tb=config_result['tb_goods_spu_search']
        self.hsw_tb=config_result['tb_hot_search_words']

        # spu_code是唯一值，用来update索引
        self.schema_spu_search=Schema(
                # spu_name=TEXT(stored=True, analyzer=jieba.analyse.ChineseAnalyzer())
                # 加上单字索引--不能同时用多个分词器
                # spu_name=TEXT(stored=True, analyzer=jieba.analyse.ChineseAnalyzer() | NgramTokenizer(minsize=1, maxsize=1))
                # spu_name=TEXT(stored=True, analyzer=jieba.analyse.ChineseAnalyzer())
                # , shop_name=TEXT(stored=True, analyzer=jieba.analyse.ChineseAnalyzer())
                spu_name=TEXT(stored=True, analyzer=ChineseAnalyzerMerge())
                # spu_name = TEXT(stored=True, analyzer=ChineseAnalyzer())
                # , shop_name=TEXT(stored=True, analyzer=ChineseAnalyzer())
                , shop_name=TEXT(stored=True, analyzer=ChineseAnalyzerMerge())
                , goods_short_edit=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_first=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_second=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_third=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_third_edit=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , goods_brand=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_code=ID(stored=True, unique=True)
                , shop_code=ID(stored=True)
                , sale_month_count=NUMERIC(numtype=int, stored=True)
                , sale_price=NUMERIC(numtype=float, stored=True)
                , updated_time_dot=STORED)

        self.search_mysql_col = "a.spu_code, a.spu_name, a.goods_short_edit\
            ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            ,a.spu_cate_third, a.spu_cate_third_edit\
            ,a.sale_price, a.sale_month_count, a.shop_name, a.shop_code, a.updated_time_dot"

        self.area_code_quanwang_sc = "2020032600001"
        self.area_code_quanwang_test = "A2020032600001"

        pathDir=os.path.dirname(__file__)
        self.path_index_all = os.path.join(pathDir, 'index_all')

        self.path_index_spu_search = os.path.join(pathDir, 'index_all', 'index_spu_search','index')
        self.path_index_shop_info = os.path.join(pathDir, 'index_all', 'index_shop_info')

        self.pos_weighting = scoring.FunctionWeighting(IndexUpdate.pos_score_fn)

    # def clean_index(dirname):
    #   # Always create the index from scratch
    #   ix = create_in(dirname, schema=get_schema())
    #   writer = ix.writer()
    #
    #   # Assume we have a function that gathers the filenames of the
    #   # documents to be indexed
    #   for path in my_docs():
    #     add_doc(writer, path)
    #
    #   writer.commit()

    @staticmethod
    def pos_score_fn(searcher, fieldname, text, matcher):
        # 静态函数，可以在代码的上面部分调用。否则会找不到函数
        if fieldname == 'goods_short_edit':
            score = 2.0
        elif fieldname == 'spu_name':
            score = 0.35
        else:
            score = 0.01
        # spu_score_weight = {'spu_short_score': 2.0, 'spu_name_score': 0.35, 'spu_brand_score': 0.10,
        #                     'spu_cate_third_edit': 0.36}

        return score

    # def index_my_mysql_shop(self, clean=True):
    #     """
    #     店铺索引
    #     """
    #
    #     def goods_spu_search_mysql_total():
    #         """
    #         读取mysql的列，设置whoosh的schema，
    #         把mysql的每一条数据当做文件，全量更新到whoosh索引
    #         只连接一次mysql，多次执行
    #         """
    #         a_col = "a.shop_name, a.shop_code, a.shop_level"
    #         dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
    #         mysqlCmd = "SELECT %s FROM %s a " % (a_col, "cb_shop_info")
    #         dataDf = pd.read_sql(mysqlCmd, dbc)
    #         dbc.close()
    #         return dataDf
    #
    #
    #     # whoosh如何输入query的list，如shop_code进行搜索？
    #     # 比较mysql的时间
    #
    #     dataDf=goods_spu_search_mysql_total()
    #     if dataDf.empty:
    #         return
    #
    #     path_index=self.path_index_shop_info
    #     if not os.path.exists(path_index):
    #         os.makedirs(path_index)
    #     ix=create_in(path_index, self.schema_spu_search)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
    #     # 按照schema信息，增加索引文档
    #     # writer=ix.writer()
    #     writer = AsyncWriter(ix)
    #     # Always create the index from scratch
    #     # ix = index.create_in(dirname, schema=get_schema())
    #     # writer = ix.writer()
    #     # Assume we have a function that gathers the filenames of the
    #     # documents to be indexed
    #     for index, row in dataDf.iterrows():
    #         updated_time_t=IndexUpdate.getmtime_of_timestamp(str(row['updated_time']))
    #         writer.add_document(spu_code=row['spu_code']
    #                             , goods_short_edit=row['goods_short_edit']
    #                             , updated_time=updated_time_t
    #                             , shop_code=row['shop_code'])
    #     writer.commit(optimize=True)  # 还是会有两个seg文件
    #     # writer.commit(optimize=False)##会有多个seg文件
    #     # print("finish writer")

    def index_my_mysql(self, clean=False):
        logger_search.info("start incre_update_index_mysql")

        def area_code_init():
            # 小区所有编码初始化
            # dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
            mysqlCmd = "SELECT distinct area_code FROM %s a " % (self.gs_tb)
            dataDf = pd.read_sql(mysqlCmd, dbc)
            acs = dataDf['area_code']
            area_codes = list(acs)
            area_codes += ['-1']  # 加入默认小区编码
            # dbc.close()
            return area_codes

        dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
        area_codes = area_code_init()
        # area_code='-1'
        try:
            if clean:
                # 先全量删除文件夹index_all
                if os.path.exists(self.path_index_all):
                    shutil.rmtree(self.path_index_all)  # 递归删除一个目录以及目录内的所有内容

                for ac in area_codes:
                    # if ac != '-1':
                    #     continue
                    self.clean_spu_index_area(ac, dbc)
            else:
                spu_info_dict=self.mysql_info_init(dbc)
                for ac in area_codes: ##应该删除不用的area_code索引文件夹
                    # if ac != '-1':
                    #     continue
                    self.incremental_spu_index(ac, dbc, spu_info_dict)
        except Exception as e:
            print("error:", traceback.format_exc())
            print("error:", e)
        finally:
            dbc.close()

        logger_search.info("finish incre_update_index_mysql")
        # print('function end : es index finished')

    def mysql_info_init(self, dbc):

        a_col = "a.spu_code, a.updated_time_dot"
        mysqlCmd = "SELECT %s FROM %s a " % (a_col, self.gss_tb)
        dataDf = pd.read_sql(mysqlCmd, dbc)  ##, parse_dates=True
        spu_info_dict={}
        for index, row in dataDf.iterrows():
            # updated_time_t = IndexUpdate.getmtime_of_timestamp(str(row['updated_time_dot']))
            try:
                updated_time_t = IndexUpdate.getmtime_of_timestamp(str(row['updated_time_dot']))
            except Exception as e:
                # time_loc = time.strftime("%Y-%m-%d %H:%M:%S.%f", time.localtime(time.time()))
                time_loc=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                updated_time_t = IndexUpdate.getmtime_of_timestamp(time_loc)

            spu_info_dict[row['spu_code']]=updated_time_t
        return spu_info_dict

    def clean_spu_index_area(self, area_code, dbc):

        def goods_spu_search_mysql_total(area_code):
            """
            读取mysql的列，设置whoosh的schema，
            把mysql的每一条数据当做文件，全量更新到whoosh索引
            只连接一次mysql，多次执行
            """
            # a_col = "a.spu_code, a.spu_name, a.goods_short_edit\
            #     ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            #     ,a.spu_cate_third, a.spu_cate_third_edit\
            #     ,a.sale_price, a.sale_month_count, a.shop_name, a.shop_code, a.updated_time"

            if area_code == '-1':
                mysqlCmd = "SELECT %s FROM %s a  where a.goods_status=1 " % (self.search_mysql_col, self.gss_tb)
            else:
                mysqlCmd = "SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                            and b.area_code in ('%s','%s','%s') \
                            and a.goods_status=1 GROUP BY a.spu_code" % (self.search_mysql_col, self.gss_tb, self.gs_tb, area_code, self.area_code_quanwang_sc, self.area_code_quanwang_test)

                # mysqlCmd = "SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                #             and b.area_code='%s' \
                #             and a.goods_status=1 " % (self.search_mysql_col, self.gss_tb, self.gs_tb, area_code)

            # 信我家智慧社区虚拟社区的商品是全网可见的。。。
            # area_code = 2020032600001
            dataDf = pd.read_sql(mysqlCmd, dbc)
            return dataDf

        dataDf=goods_spu_search_mysql_total(area_code)
        if dataDf.empty:
            return

        path_index=self.path_index_spu_search+"_"+area_code
        if not os.path.exists(path_index):
            os.makedirs(path_index)
        ix=create_in(path_index, self.schema_spu_search)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
        # 按照schema信息，增加索引文档
        # writer=ix.writer()
        writer = AsyncWriter(ix)
        # Always create the index from scratch
        # ix = index.create_in(dirname, schema=get_schema())
        # writer = ix.writer()
        # Assume we have a function that gathers the filenames of the
        # documents to be indexed
        for index, row in dataDf.iterrows():
            writer = self.writer_to_index(row, writer)
            # updated_time_t=IndexUpdate.getmtime_of_timestamp(str(row['updated_time']))
            # writer.add_document(spu_code=row['spu_code']
            #                     , spu_name=row['spu_name']
            #                     , shop_name=row['shop_name']
            #                     , goods_short_edit=row['goods_short_edit']
            #                     , spu_cate_third_edit=row['spu_cate_third_edit']
            #                     , updated_time=updated_time_t
            #                     , shop_code=row['shop_code'])
        writer.commit(optimize=True)  # 还是会有两个seg文件
        # writer.commit(optimize=False)##会有多个seg文件
        # print("finish writer")

    # def getmtime_of_timestamp(self, timestr):静态方法
    @staticmethod
    def getmtime_of_timestamp(timestr):
        timestr_dot = timestr[-6:]  ##取出后6位
        # timestr=dataDf['updated_time'].iloc[0].apply(lambda x: time.mktime(time.strptime(x, '%Y-%m-%d %H:%M:%S')))
        # timestr = dataDf['updated_time'].apply(lambda x: time.mktime(time.strptime(x, '%Y-%m-%d %H:%M:%S')))

        # 转换成时间数组
        timeArray = time.strptime(timestr, "%Y-%m-%d %H:%M:%S.%f")  ##???小数？？
        # print(timeArray, type(timeArray), sep="\n")
        # 转换成时间戳
        timestamp = time.mktime(timeArray)
        timestamp_dot = timestamp + int(timestr_dot) / (10 ** 6)
        # print(timestamp, timestamp_dot, sep='\n')
        # print(type(timestamp), type(timestamp_dot), sep="\n")

        # data['timestamp'] = data['OCC_TIM'].apply(lambda x: time.mktime(time.strptime(x, '%Y-%m-%d %H:%M:%S')))
        # timestamp = int(time.mktime(timeArray.timetuple()) * 1000.0 + timeArray.microsecond / 1000.0)

        # print(type(timestamp), timestamp, sep='')
        # modtime  Out[6]: 1589938345.6122835
        # 秒后面保留7位有效数字
        # "Unknown table 'hisense_bi_data_rec.cb_goods_spu_search_test'")
        return timestamp_dot

    @staticmethod
    def writer_to_index(row, writer):

        def query_text_transfer(gse_raw):
            gse_raw = gse_raw.replace(',', ' ')
            gse_raw = gse_raw.replace('，', ' ')
            gse_raw_list=gse_raw.split(" ")
            key_word_trans = set()
            for i in gse_raw_list:
                if len(i)==0:
                    continue
                if i in synonym_dict:
                    syn_set = synonym_dict[i]
                    key_word_trans.update(syn_set)
                else:
                    key_word_trans.update({i})###注意：这里不能key_word_trans.update("你好")，而应该是key_word_trans.update({"你好"})

            gse_syn=" ".join(key_word_trans)
            return gse_syn

        # updated_time_t = IndexUpdate.getmtime_of_timestamp(str(row['updated_time_dot']))
        try:
            updated_time_t = IndexUpdate.getmtime_of_timestamp(str(row['updated_time_dot']))
        except Exception as e:
            # time_loc = time.strftime("%Y-%m-%d %H:%M:%S.%f", time.localtime(time.time()))
            time_loc = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            updated_time_t = IndexUpdate.getmtime_of_timestamp(time_loc)

        gse_raw=row['goods_short_edit']
        gse_syn=query_text_transfer(gse_raw)##同义词转写

        # 对第三类别字段也要转写，比如“冰淇淋”
        sct_raw=row['spu_cate_third']
        sct_syn=query_text_transfer(sct_raw)##同义词转写

        writer.add_document(updated_time_dot=updated_time_t
                            , spu_code=row['spu_code']
                            , spu_name=row['spu_name']
                            , shop_name=row['shop_name']
                            , goods_brand=row['goods_brand']
                            , goods_short_edit=gse_syn
                            , spu_cate_first=row['spu_cate_first']
                            , spu_cate_second=row['spu_cate_second']
                            , spu_cate_third=sct_syn
                            , spu_cate_third_edit=row['spu_cate_third_edit']
                            , shop_code=row['shop_code']
                            , sale_month_count=row['sale_month_count']
                            , sale_price=row['sale_price'])
        return writer

    def incremental_spu_index(self, area_code, dbc, spu_info_dict):

        def my_spus():
            a_col = "a.spu_code"
            if area_code == '-1':
                mysqlCmd = "SELECT %s FROM %s a where a.goods_status=1 " % (a_col, self.gss_tb)
            else:
                mysqlCmd = "SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                            and b.area_code='%s' \
                            and a.goods_status=1 " % (a_col, self.gss_tb, self.gs_tb, area_code)

            dataDf = pd.read_sql(mysqlCmd, dbc)
            spu_codes = dataDf['spu_code'].values
            return spu_codes

        def goods_spu_search_mysql_spu(spu_code):
            """
            spu_code对应的商品信息
            """
            # a_col = "a.spu_code, a.spu_name, a.goods_short_edit\
            #     ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            #     ,a.spu_cate_third, a.spu_cate_third_edit\
            #     ,a.sale_price, a.sale_month_count, a.shop_name, a.shop_code, a.updated_time"

            mysqlCmd = "SELECT %s FROM %s a where a.spu_code='%s' " % (self.search_mysql_col, self.gss_tb, spu_code)
            dataDf = pd.read_sql(mysqlCmd, dbc)
            return dataDf
        """
        增量遍历
        """
        my_spu_codes=my_spus()
        # spu_info_dict = goods_spu_search_mysql_spus(dbc)
        # timestamp_dot=getmtime("1")
        path_index = self.path_index_spu_search + "_" + area_code  # open的时候就不用创建目录了
        # 如果不存在，需要全量创建索引
        if not os.path.exists(path_index):
            # os.makedirs(path_index)
            self.clean_spu_index_area(area_code, dbc)
            return
        ix = open_dir(path_index)
        # The set of all paths in the index
        indexed_paths = set()
        # The set of all paths we need to re-index
        to_index = set()
        """
        index索引文件和mysql文件的同步
        """
        with ix.searcher() as searcher:
            # writer = ix.writer()
            writer = AsyncWriter(ix)
            # Loop over the stored fields in the index
            for fields in searcher.all_stored_fields():
                spu_code = fields['spu_code']
                indexed_paths.add(spu_code)
                # if not exists_spu(spu_code):
                if spu_code not in my_spu_codes:
                    # print(spu_code,type(spu_code))
                    # print(type(my_spu_codes))
                    # This file was deleted since it was indexed
                    writer.delete_by_term('spu_code', spu_code)
                else:
                    # Check if this file was changed since it
                    # was indexed
                    indexed_time_mtime = fields['updated_time_dot']
                    # timestr = str(indexed_time)
                    # print(type(indexed_time), type(timestr), sep=",")
                    # indexed_time_mtime = self.getmtime_of_timestamp(timestr)
                    # 创建更新时间的索引
                    # mtime = getmtime(spu_code)
                    # mtime = spu_info_dict[spu_code]
                    # 默认更新
                    if spu_code in spu_info_dict:
                        mtime = spu_info_dict[spu_code]
                    else:
                        mtime = 9592812324

                    if mtime > indexed_time_mtime:
                        # print(mtime, indexed_time_mtime)
                        # The file has changed, delete it and add it to the list of
                        # files to reindex
                        writer.delete_by_term('spu_code', spu_code)
                        to_index.add(spu_code)
            # Loop over the files in the filesystem
            # Assume we have a function that gathers the filenames of the
            # documents to be indexed
            for spu_code in my_spu_codes:
                if spu_code in to_index or spu_code not in indexed_paths:
                    # This is either a file that's changed, or a new file
                    # that wasn't indexed before. So index it!
                    # add_doc(writer, path)
                    dataDf=goods_spu_search_mysql_spu(spu_code)
                    for index, row in dataDf.iterrows():
                        writer=self.writer_to_index(row, writer)

            writer.commit(optimize=True)
            # writer.commit(optimize=False)
        """
        别忘了关闭mysql连接        
        """
        # dbc.close()

    # def get_schema():
    #   return Schema(path=ID(unique=True, stored=True), content=TEXT)
    #
    
    # def add_doc(writer, path):
    #   fileobj = open(path, "rb")
    #   content = fileobj.read()
    #   fileobj.close()
    #   writer.add_document(path=path, content=content)
    """
    现在，对于一小部分文档集合，每次从头开始索引实际上可能足够快。但是对于大型集合，您只需要让脚本重新索引已更改的文档。
    
    首先，我们需要存储每个文档的上次修改时间，以便检查文件是否已更改。在本例中，我们将使用mtime来简化：
    
    """
    # def get_schema():
    #   return Schema(path=ID(unique=True, stored=True), time=STORED, content=TEXT)
    
    # def add_doc(writer, path):
    #   fileobj = open(path, "rb")
    #   content = fileobj.read()
    #   fileobj.close()
    #   modtime = os.path.getmtime(path)
    #   writer.add_document(path=path, content=content, time=modtime)
    # 现在我们可以修改脚本以允许“清理”（从头开始）或增量索引：
    
    # def index_my_docs(dirname, clean=False):
    #   if clean:
    #     clean_index(dirname)
    #   else:
    #     incremental_index(dirname)
    
    # def index_my_mysql(self, clean=False):
    #     path_index_spu_search=self.path_index_spu_search
    #     if clean:
    #         self.clean_spu_index(path_index_spu_search)
    #     else:
    #         self.incremental_spu_index(path_index_spu_search)
    """
    这个 incremental_index 功能：
    循环访问当前索引的所有路径。
    如果任何文件不再存在，请从索引中删除相应的文档。
    如果文件仍然存在，但已被修改，请将其添加到要重新索引的路径列表中。
    如果该文件存在，不管它是否被修改过，请将其添加到所有索引路径的列表中。
    循环访问磁盘上文件的所有路径。
    如果一个路径不在所有索引路径集中，那么该文件是新的，我们需要索引它。
    如果路径位于要重新索引的路径集中，则需要对其进行索引。
    否则，我们可以跳过索引文件。
    """
    def create_mysql_goods_table_test(self):
        
        """
        测试建表
        """
        dbc = pymysql.connect(host=self.ip,user=self.user,password=self.password,db=self.db,port=self.port)
        cur = dbc.cursor()
        try:
            cur.execute(mc.drop_spu_table)
            cur.execute(mc.create_spu_table)
        
            for i in range(5):
                insert_mysql=mc.insert_spu_table % (str(i),333,"苹果_"+str(i),'1','1','1','1','1','1','1','1','1',222,'1') 
                cur.execute(insert_mysql) #执行sql语句
            dbc.commit()

            # dataDf=pd.read_sql(insert_mysql, dbc)
        except Exception as e:
            print("hehehe", traceback.format_exc())
        finally:
            cur.close()
            dbc.close()    

        # print("hello")

    def insert_mysql_goods_table_test(self):
        """
        测试建表
        """
        dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
        cur = dbc.cursor()
        try:
            # cur.execute(mc.drop_spu_table)
            # cur.execute(mc.create_spu_table)

            for i in range(5):
                insert_mysql = mc.insert_spu_table % (
                str(i+100), 333, "苹果_insert_" + str(i), '1', '1', '1', '1', '1', '1', '1', '1', '1', 222, '1')
                cur.execute(insert_mysql)  # 执行sql语句
            dbc.commit()
            # dataDf=pd.read_sql(insert_mysql, dbc)
        except Exception as e:
            print("hehehe", traceback.format_exc(), sep="\n")
        finally:
            cur.close()
            dbc.close()

    def add_mysql_column(self):
        dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
        cur = dbc.cursor()
        try:
            add_mysql = mc.add_column
            cur.execute(add_mysql)  # 执行sql语句
            dbc.commit()
        except Exception as e:
            print("hehehe", traceback.format_exc(), sep="\n")
        finally:
            cur.close()
            dbc.close()

    def open_index_spu_search(self, area_code):
        path_index=self.path_index_spu_search+"_"+area_code
        sc_online=open_dir(path_index).searcher()
        myquery=Every()
        qp = qparser.MultifieldParser(fieldnames=["goods_short_edit"], schema=self.schema_spu_search)
        # qp = qparser.MultifieldParser(fieldnames=["spu_code"], schema=self.schema_spu_search)
        myquery = qp.parse("苹果")
        results=sc_online.search(myquery, limit=None)#limit=None返回所有结果
        # results=sc_online.search(myquery, limit=12)#limit=None返回所有结果
        if len(results)>0:
            for hit in results:
                print('hit:\n', hit)
                # print('content:\n', hit['content'])
                print('score:\n', hit.score)
        else:
            print('empty')
        # results_online_list=[str(i, encoding = "utf-8") for i in sc_online.lexicon("goods_short")]
        print('number of results:', len(results))#number of results: 3091
        sc_online.close()

    # def open_index_spu_search(self, arg_dict):
    #
    #     def page_json_default():
    #         data_dict = {
    #             "searchRult": "3",
    #             "pageNum": 1,
    #             "pageSize": 10,
    #             "size": 0,
    #             "startRow": 0,
    #             "endRow": 0,
    #             "total": -1,
    #             "pages": 1,
    #             "list": [],
    #             "prePage": 0,
    #             "nextPage": 0,
    #             "isFirstPage": True,
    #             "isLastPage": True,
    #             "hasPreviousPage": False,
    #             "hasNextPage": False,
    #             "navigatePages": 8,
    #             "navigatepageNums": [
    #                 1
    #             ],
    #             "navigateFirstPage": 1,
    #             "navigateLastPage": 1,
    #             "firstPage": 1,
    #             "lastPage": 1
    #         }
    #
    #         page_dict = {
    #             "resultCode": "0",
    #             "msg": "操作成功",
    #             "data": data_dict
    #             #               'testSpu':spu_code_search##测试分页的正确性
    #         }
    #         #    print(page_dict)
    #         page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
    #         return page_json
    #
    #     def page_json_default_shop():
    #
    #         data_dict = {
    #             "current": 0,
    #             "pages": 0,
    #             "records": [],
    #             "searchCount": True,
    #             "size": 0,
    #             "total": 0}
    #
    #         page_dict = {
    #             "resultCode": "0",
    #             "msg": "操作成功",
    #             "data": data_dict
    #             #               'testSpu':spu_code_search##测试分页的正确性
    #         }
    #         page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
    #         return page_json
    #
    #     def page_genertate(spu_code_search):
    #
    #         rows_str = arg_dict['rows']
    #         page = arg_dict['page']
    #
    #         rows = int(rows_str)
    #         goods_num = len(spu_code_search)
    #         page_size = int(np.ceil(goods_num / rows))  # rows=10##默认一页10个商品
    #         #    page_size=9
    #         #    spu_code_search={'vvd','afas'}
    #         # spu_code_search = list(spu_code_search)
    #
    #         pageNum_int = int(page)  ##第几页，页码
    #         if pageNum_int == 1:
    #             isFirstPage = True
    #             hasPreviousPage = False
    #         else:
    #             isFirstPage = False
    #             hasPreviousPage = True
    #
    #         if pageNum_int == page_size:
    #             isLastPage = True
    #             hasNextPage = False
    #         else:
    #             isLastPage = False
    #             hasNextPage = True
    #
    #         ##暂时用page从1开始
    #         startRow = (pageNum_int - 1) * rows
    #         ##最后一页
    #         if isLastPage:
    #             endRow = goods_num
    #         else:
    #             endRow = pageNum_int * rows
    #         goods_code_page = spu_code_search[startRow:endRow]
    #         ##用join更简单
    #         spuCodes = ",".join(goods_code_page)
    #         data_dict = {
    #             #        "searchRult": "1",
    #             "searchRult": "1",
    #             "pageNum": pageNum_int,
    #             "pageSize": page_size,
    #             "size": 1,
    #             "startRow": startRow + 1,  # 从1开始
    #             "endRow": endRow,
    #             "total": 1,
    #             "pages": 1,
    #             "spuCodes": spuCodes,
    #             "prePage": 0,
    #             "nextPage": 0,
    #             "isFirstPage": isFirstPage,
    #             "isLastPage": isLastPage,
    #             "hasPreviousPage": hasPreviousPage,
    #             "hasNextPage": hasNextPage,
    #             "navigatePages": 8,
    #             "navigatepageNums": [1],
    #             "navigateFirstPage": 1,
    #             "navigateLastPage": 1,
    #             "firstPage": 1,
    #             "lastPage": 1}
    #         #    searchRult  1 完全匹配结果  2 不完全匹配结果 3 没有结果
    #         page_dict = {
    #             "resultCode": "0",
    #             "msg": "操作成功",
    #             "data": data_dict
    #             #               'testSpu':spu_code_search##测试分页的正确性
    #         }
    #         #    print(page_dict)
    #         page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
    #         #    print(page_json)
    #         return page_json
    #
    #     def page_genertate_shop(spu_code_search):
    #
    #         rows_str = arg_dict['rows']
    #         page = arg_dict['page']
    #
    #         rows = int(rows_str)
    #         goods_num = len(spu_code_search)
    #         page_size = int(np.ceil(goods_num / rows))  # rows=10##默认一页10个商品
    #         #    page_size=9
    #         #    spu_code_search={'vvd','afas'}
    #         # spu_code_search = list(spu_code_search)
    #
    #         pageNum_int = int(page)  ##第几页，页码
    #         if pageNum_int == 1:
    #             isFirstPage = True
    #             hasPreviousPage = False
    #         else:
    #             isFirstPage = False
    #             hasPreviousPage = True
    #
    #         if pageNum_int == page_size:
    #             isLastPage = True
    #             hasNextPage = False
    #         else:
    #             isLastPage = False
    #             hasNextPage = True
    #
    #         ##暂时用page从1开始
    #         startRow = (pageNum_int - 1) * rows
    #         ##最后一页
    #         if isLastPage:
    #             endRow = goods_num
    #         else:
    #             endRow = pageNum_int * rows
    #         goods_code_page = spu_code_search[startRow:endRow]
    #         data_dict = {
    #             "current": pageNum_int,
    #             "pages": page_size,
    #             "records": goods_code_page,
    #             "searchCount": True,
    #             "size": 0,
    #             "total": 0}
    #         page_dict = {
    #             "resultCode": "0",
    #             "msg": "操作成功",
    #             "data": data_dict
    #             #               'testSpu':spu_code_search##测试分页的正确性
    #         }
    #         page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
    #         return page_json
    #
    #     def rankbyshop(shop_code_dict):
    #
    #         # 用mysql根据销量和价格排序
    #         shop_code_list=list(shop_code_dict.keys())
    #         if len(shop_code_list) == 0:
    #             shop_group_list = []
    #             return shop_group_list
    #
    #         if sort_method == '0':
    #             # shop_group_list=rank_by_goods_scores(shop_code_dict)
    #             shop_group_list=[{k:v} for k,v in shop_code_dict.items()]
    #             return shop_group_list
    #
    #         hot_list_str = ",".join(shop_code_list)
    #         if sort_method == '3':  ##店铺评分
    #             mysqlCmd = "SELECT a.shop_code FROM cb_shop_rank_info AS a \
    #                 WHERE shop_code IN (%s) ORDER BY a.shop_level DESC" % (hot_list_str)
    #         elif sort_method == '4':
    #             mysqlCmd = "SELECT a.shop_code FROM cb_shop_rank_info AS a \
    #                 WHERE shop_code IN (%s) ORDER BY a.shop_level " % (hot_list_str)
    #         elif sort_method == '1':  ##价格从低到高
    #             mysqlCmd = "SELECT a.spu_code FROM goods_spu_search AS a \
    #                 WHERE spu_code IN (%s) ORDER BY a.sale_price" % (hot_list_str)
    #         elif sort_method == '2':  ##价格从高到低
    #             mysqlCmd = "SELECT a.spu_code FROM goods_spu_search AS a \
    #                 WHERE spu_code IN (%s) ORDER BY a.sale_price DESC" % (hot_list_str)
    #
    #         dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
    #         dataDf = pd.read_sql(mysqlCmd, dbc)
    #         shop_list_sort = dataDf['shop_code'].tolist()
    #         dbc.close()
    #
    #         shop_group_list=[{k:shop_code_dict[k]} for k in shop_list_sort]
    #         return shop_group_list
    #
    #     def search_i_filter(query_all, query_important, sc_online):
    #
    #         results_all=search_ni(query_all, sc_online)
    #         if len(results_all)==0:
    #             return results_all
    #
    #         qp = qparser.MultifieldParser(fieldnames=["goods_short_edit", "spu_name"], schema=self.schema_spu_search,
    #                                       group=qparser.AndGroup)
    #         myquery_i = qp.parse(query_important)
    #         results = sc_online.search(myquery_i, limit=None, scored=False, sortedby=False)  # limit=None返回所有结果
    #         # results=sc_online.boolean_context()不行
    #         # scored - - 是否得分。sortedby.如果两者。scored = False和sortedby = None
    #         # 结果将按任意顺序排列，但通常计算速度比计分或排序结果快。
    #         # print(results['spu_code'])
    #
    #         # sorting模块
    #         # paths = sorting.FieldFacet("spu_code", reverse=True)
    #         # results=sc_online.search(myquery, limit=None, sortedby=paths, groupedby="shop_code")#limit=None返回所有结果
    #         # results=sc_online.search(myquery, limit=None, groupedby="shop_code")#limit=None返回所有结果
    #
    #         # 输出结果的顺序：先按shop_code组(数字的大小顺序)，再在组内按照score
    #         # shop_code = sorting.FieldFacet("shop_code", reverse=True)
    #         # scores = sorting.ScoreFacet()
    #         # results = sc_online.search(myquery, limit=None, groupedby="shop_code", sortedby=[shop_code, scores])
    #         # results=sc_online.search(myquery, limit=12)#limit=None返回所有结果
    #         # print(type(results), results)
    #
    #         """
    #         过滤
    #         """
    #         # otherdocs = results.docs()
    #         # print(otherdocs)
    #         # for i in results_all.top_n:
    #         #     print(i, i[0], i[1])
    #         # results_filter=[i for i in results_all.top_n ]
    #         spu_codes_list = [hit['spu_code'] for hit in results]
    #         results_filter = [i for i in results_all if i['spu_code'] in spu_codes_list]
    #         # results_all.filter(results)#也可以
    #         return results_filter
    #
    #     def search_i(query_important, sc_online):
    #
    #         qp = qparser.MultifieldParser(fieldnames=["goods_short_edit", "spu_name"], schema=self.schema_spu_search,
    #                                       group=qparser.AndGroup)
    #         myquery_i = qp.parse(query_important)
    #         results = sc_online.search(myquery_i, limit=None)  # limit=None返回所有结果
    #         return results
    #
    #     def search_ni(query_all, sc_online):
    #
    #         qp_ni = qparser.MultifieldParser(fieldnames=["goods_short_edit", "spu_name"], schema=self.schema_spu_search,
    #                                          group=qparser.OrGroup)
    #         myquery_ni = qp_ni.parse(query_all)
    #         results_all = sc_online.search(myquery_ni, limit=None)
    #         print(results_all)
    #         for hit in results_all:
    #             print('hit:\n', hit)
    #             print('score:\n', hit.score)
    #
    #         return results_all
    #
    #     # query_all=arg_dict['query']
    #     query_ner_type=arg_dict['query_ner_type']
    #     # query_all = "苹果 苹果啊 漂亮"
    #     area_code=arg_dict['area_code']
    #     sort_method=arg_dict['sort_method']
    #     search_dim=arg_dict['search_dim']
    #     path_index=self.path_index_spu_search+"_"+area_code
    #     if not os.path.exists(path_index):
    #         return ""
    #     else:
    #         sc_online=open_dir(path_index).searcher(weighting=self.pos_weighting)
    #     # sc_online = open_dir(path_index).searcher(weighting=pos_weighting_not_important)
    #     # qp = qparser.MultifieldParser(fieldnames=["goods_short_edit"], schema=self.schema_spu_search, group=qparser.OrGroup)
    #     # mparser=qparser.MultifieldParser(fieldnames=["title", "content"], schema=schema, group=qparser.AndGroup)
    #     # query_not_important = "漂亮"
    #     # query_important = "苹果 苹果啊 羊角蜜"
    #     # query_important = "苹果 苹果啊"
    #     query_not_important = " ".join(query_ner_type['schemas_not_important'])
    #     query_important = " ".join(query_ner_type['schemas_important'])
    #     query_all = " ".join(query_ner_type['schemas_not_important']+query_ner_type['schemas_important'])
    #
    #     print(query_all,query_important,query_not_important,sep="\n")
    #
    #     if len(query_not_important)==0:##??
    #         results=search_i(query_important, sc_online)
    #     elif len(query_important)==0:
    #         results = search_ni(query_not_important, sc_online)
    #     else:
    #         results = search_i_filter(query_all, query_important, sc_online)
    #
    #     print('number of results:', len(results))  # number of results: 3091
    #     if len(results) == 0:
    #         if search_dim == '1':
    #             page_json = page_json_default()
    #         elif search_dim == '2':
    #             page_json = page_json_default_shop()
    #         else:
    #             page_json=""
    #     else:
    #         if search_dim == '1':
    #             spu_codes_list=[hit['spu_code'] for hit in results]
    #             print(spu_codes_list)
    #             for hit in results:
    #                 print(hit, hit.score, sep="\n")
    #             page_json=page_genertate(spu_codes_list)
    #         elif search_dim == '2':
    #             shop_code_dict={}
    #             for hit in results:
    #                 print('hit:\n', hit)
    #                 print('score:\n', hit.score)
    #                 if hit['shop_code'] in shop_code_dict:
    #                     shop_code_dict[hit['shop_code']].append(hit['spu_code'])
    #                 else:
    #                     shop_code_dict[hit['shop_code']]=[hit['spu_code']]
    #             shop_group_list=rankbyshop(shop_code_dict)
    #             print(shop_group_list)
    #             page_json = page_genertate_shop(shop_group_list)
    #             # shop_code_dict=results.groups()
    #             # shop_code_dict = results.groups("shop_code")
    #             # print(type(shop_code_dict), shop_code_dict)
    #             # print(list(shop_code_dict.keys()), list(shop_code_dict.values()), sep="\n")
    #
    #             # 本身返回的是一个字典，没有顺序。可以取出第一个记录，得到排序score，然后按照score排序分组
    #             # 可能是Python的字典的默认排序规则--自己测试一下字典的排序，应该是按照创建key的顺序排序的
    #             # 如果score相同，再按照匹配数量？
    #             # --好像是按照数据的更新时间排序的，最新的数据对应的分组靠前--等价于文档编号的顺序doclist！！！
    #             # 最终证明：有两种排序，
    #             # 分组的排序是按照首个编号的大小，即修改更新时间
    #             # 组内的排序默认按照score排序的，只是在分数score相同的情况下，才按照修改时间排序
    #
    #             """
    #             返回商铺列表之后，要进行再次排序。
    #             1、按照销量--搜索另一个“商铺-销量”whoosh索引？暂时用mysql，速度还行
    #             先把商品列表的结果放到redis里面？失效期是多长时间？
    #             先从redis取，如果有的话直接调用商铺排序接口，否则重新搜索一遍！
    #             2、按照自定义的方法--组内商品相关性score排序
    #             """
    #         else:
    #             page_json=""
    #
    #     sc_online.close()
    #     return page_json


iu = IndexUpdate()
    
    
if __name__=="__main__":
    
    # iu.create_mysql_goods_table_test()
    # iu.insert_mysql_goods_table_test()##D:\anaconda3\envs\test1\lib\site-packages\pymysql\cursors.py:170: Warning: (1062, "Duplicate entry '104' for key 'spu'")
    """
    先全量同步更新，再增量更新
    cb_shop_info数据同步
    """

    # iu.add_mysql_column()
    t0=time.time()
    iu.index_my_mysql(clean=True)
    iu.open_index_spu_search(area_code='-1')

    mysqlCmd = "SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                and b.area_code in ('%s','%s','%s') \
                and a.goods_status=1 GROUP BY a.spu_code" % (    iu.search_mysql_col, iu.gss_tb, iu.gs_tb, "A20190121", iu.area_code_quanwang_sc, iu.area_code_quanwang_test)
    print(mysqlCmd)

    # 验证“满减”分词
    # analyzer = ChineseAnalyzer()
    # for t in analyzer("我的好朋友是李明；我想买满减的商品苹果。满减"):
    # query="满减"
    # for t in analyzer(query):
    #     print(t.text)
    # words = jieba.lcut_for_search(query, HMM=False)
    # print(words)
    t1=time.time()
    time_used = t1 - t0
    print("test:------\n","Time consumed(s): ", time_used)


    # for query in ["白啤","白酒","白沙河"]:
    #     seg_list = list(jieba.cut_for_search(query))
    #     words=jieba.lcut_for_search(query, HMM=False)
    #
    #     print(seg_list, words, sep="\n")

