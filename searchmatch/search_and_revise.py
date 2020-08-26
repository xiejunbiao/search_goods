# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 17:52:16 2019

@author: lijiangman
"""
import os
import json
import pypinyin
# import copy
#python多次重复使用import语句时，不会重新加载被指定的模块，
#只是把对该模块的内存地址给引用到本地变量环境。
import numpy as np
import pandas as pd
import pymysql  # python3
import pickle

from whoosh import qparser, scoring, sorting
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, NUMERIC, STORED
from whoosh.analysis import StandardAnalyzer
from whoosh.query import Term

from searchhotmain.inition_config import config_result##数据库配置
from searchmatch.synonym_lib import synonym_dict
from searchmatch.analyzerbyme import jieba  # 从这里导入import，妙不可言，不用重复调用jieba了！

##只运行一次，加载一次库
####reload(jieba)#加载过其他参数load_userdict，需要重新reload

##这个词库不能使用，因为有“纯白毛巾”这个词！！分词不能把“纯白”和“毛巾”分开
##先load字典，再import analyse
##reload(analyse)#重新加载analyse模块
##停用词
pathDir=os.path.dirname(__file__)
stw_all_path = os.path.join(pathDir, 'utils', 'stop_words_all')
# analyse.set_stop_words(stw_all_path)#停用词过滤
# stopwords = [line.strip() for line in open(stw_all_path,encoding='UTF-8').readlines()]
#get user_dictionary and stop_words
# jieba.load_userdict("user_dictionary")
with open(stw_all_path, encoding='UTF-8') as f:
    stopwordsA = [line.strip() for line in f.readlines()]
# words = jieba.cut(text)
# clean words whos length<2 and with only numbers and characters
# words = [w for w in words if len(w)>1 
 # and not re.match('^[a-z|A-Z|0-9|.]*$',w)]
# clean stop_words
# words = [w for w in words if w not in stopWords]
# from jieba import lcut_for_search
##语义纠错-拼音纠错
# wpy_dict=pickle.load(open('../searchMatchV2/wpy_dict.pkl', 'rb'))  
##语音搜索专用剔除词库--估计说我想买与，应该能够转写我想买鱼
yy_stopwords=[' ','啊','你','我','他','我想','我想买','想买','想要'
              ,'要买','我想要','买点','来点','我想要买'
              ,'给我','给我点','给','我要'
              ,'我要吃','要吃','想吃','我想吃','我想要吃'
              ,'有没有', '的']

stopwords = stopwordsA + yy_stopwords

#方法3：动态修改词频
#调节单个词语的词频，使其能（不能）被分出来。
# jieba.suggest_freq('酒', tune=False) 查看源码，说明，如果无法成功，需要设置HMM=False

# shop_name=[ '海信广场百货店', '海信广场超市', '海信广场'
#            , '海信科学探索中心', '研发后勤内购福利店'
#            , '行政后勤中心员工内购店-大厦店', '行政后勤中心员工内购店-研发店']#字数大于等于4的商铺过滤
# shop_name_other=['信我家自营', '永旺东部店','网易严选专营店'] #字数大于等于2的商铺过滤
# shop_name_all=shop_name+shop_name_other##所有商铺名字


def page_json_default():
    data_dict = {
        "searchRult": "3",
        "pageNum": 1,
        "pageSize": 10,
        "size": 0,
        "startRow": 0,
        "endRow": 0,
        "total": -1,
        "pages": 1,
        "list": [],
        "prePage": 0,
        "nextPage": 0,
        "isFirstPage": True,
        "isLastPage": True,
        "hasPreviousPage": False,
        "hasNextPage": False,
        "navigatePages": 8,
        "navigatepageNums": [
            1
        ],
        "navigateFirstPage": 1,
        "navigateLastPage": 1,
        "firstPage": 1,
        "lastPage": 1
    }

    page_dict = {
        "resultCode": "0",
        "msg": "操作成功",
        "data": data_dict
        #               'testSpu':spu_code_search##测试分页的正确性
    }
    #    print(page_dict)
    # page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
    return page_dict


def page_json_default_shop():
    data_dict = {
        "current": 0,
        "pages": 0,
        "records": [],
        "searchCount": True,
        "size": 0,
        "total": 0}

    page_dict = {
        "resultCode": "0",
        "msg": "操作成功",
        "data": data_dict
        #               'testSpu':spu_code_search##测试分页的正确性
    }
    # page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
    return page_dict


class SearchGoodsEgine:
    
    def __init__(self):

        pathDir = os.path.dirname(__file__)

        path_wpy_dict = os.path.join(pathDir, 'word_pkls', 'wpy_dict.pkl')
        path_whz_dict = os.path.join(pathDir, 'word_pkls', 'whz_dict.pkl')
        path_ner_dict = os.path.join(pathDir, 'word_pkls', 'name_entity_dict.pkl')
        path_goods_cate = os.path.join(pathDir, 'word_pkls', 'goods_cate_set.pkl')

        # freqImportant=6#法国的词频11
        freqImportant = 2  # 法国的词频11
        percent_match = 0.6  # 召回的匹配数目百分比
        # percent_match=0.49#召回的匹配数目百分比--进口红酒---进口饼干排序到后面
        percent_match = 0.4  # 召回的匹配数目百分比--三个词，命中一个-草莓地区地球
        # percent_match=0.04#解决网易的按摩仪
        # spu_score_weight={'spu_short_score':1.0, 'spu_name_score':0.2, 'spu_brand_score':0.10, 'spu_cate_third_edit':0.15}
        spu_score_weight = {'spu_short_score': 2.0, 'spu_name_score': 0.35, 'spu_brand_score': 0.10,
                            'spu_cate_third_edit': 0.36}

        self.wpy_dict=pickle.load(open(path_wpy_dict, 'rb'))
        self.whz_dict=pickle.load(open(path_whz_dict, 'rb'))
        self.ner_dict=pickle.load(open(path_ner_dict, 'rb'))
        
        self.goods_cate_set=pickle.load(open(path_goods_cate,'rb'))#类别搜索
        
        self.percent_match=percent_match
        self.freqImportant=freqImportant
        self.spu_score_weight=spu_score_weight

        # self.gss_tb="bd_goods_spu_search"
        # self.gs_tb="bd_goods_scope"##小区代码和spu代码对应关系
        self.gss_tb=config_result['tb_goods_spu_search']
        self.gs_tb=config_result['tb_goods_scope']##小区代码和spu代码对应关系
        self.ip=config_result['ip']
        self.user=config_result['user']
        self.password=config_result['password']
        self.db=config_result['db']
        self.port=config_result['port']
        self.cb_shop_rank_info=config_result['cb_shop_rank_info']

        pathDir = os.path.dirname(__file__)
        self.path_index_spu_search = os.path.join(pathDir, 'index_all', 'index_spu_search', 'index')
        self.path_index_shop_info = os.path.join(pathDir, 'index_all', 'index_shop_info')
        # 在搜索的时候，由于query都是空格的，就不用jieba分词了，直接英文分词---对于过滤的shop_code，不用加入schema
        self.schema_spu_search=Schema(
                spu_name=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , shop_name=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , goods_short_edit=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_first=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_second=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_third=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , spu_cate_third_edit=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                , goods_brand=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))

                # , sale_price=NUMERIC(numtype=float, stored=True)##如何不加入schema
                )

        self.search_mysql_col = "a.spu_code, a.spu_name, a.goods_short_edit\
            ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            ,a.spu_cate_third, a.spu_cate_third_edit\
            ,a.sale_price, a.sale_month_count, a.shop_name, a.shop_code, a.updated_time_dot"

        self.fieldnames=["goods_short_edit", "spu_name", "shop_name"
                         , "goods_brand", "spu_cate_third", "spu_cate_third_edit"]
        # spu_cate_first': '乳品烘焙', 'spu_cate_second': ' 黄油奶酪 ', ' spu_cate_third': ' 黄油
        # 删除第三类别，否则搜索“黄油饼干”会把“黄油”类都给出来
        # self.fieldnames_cate=["spu_cate_first", "spu_cate_second", "spu_cate_third"]
        self.fieldnames_cate=["spu_cate_first", "spu_cate_second"]

        def pos_score_fn(searcher, fieldname, text, matcher):
            # 静态函数，可以在代码的上面部分调用。否则会找不到函数
            if fieldname == 'goods_short_edit':
                score = 2.0
            elif fieldname == 'spu_name':
                score = 0.35
            elif fieldname =='spu_cate_third_edit':
                score = 0.36
            elif fieldname =='shop_name':
                score = 0.30
            else:
                score = 0.01
            # spu_score_weight = {'spu_short_score': 2.0, 'spu_name_score': 0.35, 'spu_brand_score': 0.10,
            #                     'spu_cate_third_edit': 0.36}

            return score
        self.pos_weighting = scoring.FunctionWeighting(pos_score_fn)

        # 注意：必须有pass或者其他的语句，否则下面会有格式错误！！太尼玛智能了
        # pass

    def keyword_extract(self, arg_dict):##用户请求的关键词提取
        ##商铺同义词？
        arg_dict['query']=arg_dict['query'].replace('佳世客','永旺')
        self.arg_dict=arg_dict
        query=self.arg_dict['query']
       
        # clean stop_words
        words=jieba.lcut_for_search(query, HMM=False)
        # print(query, words)
        words = [w for w in words if w not in stopwords]
        query_key_word_set = set(words)
        
        word_cut_f = jieba.cut(query, cut_all=False)
        words = [w for w in word_cut_f if w not in stopwords]
        words_join="".join(words)
        self.words_join=words_join

        query_key_word_set.add(words_join)#加入整体词，可以搜索goods_short_edit字段

        # if words_join not in stopwords:##为了防止“手撕”、“刀月”被分开，把用户原始输入加入集合
        #     query_key_word_set.add(words_join)

        """已经把空格加入停用词"""
        # while '' in query_key_word_set:
        #     query_key_word_set.remove('')
        # ##空格
        # while ' ' in query_key_word_set:
        #     query_key_word_set.remove(' ')
    
        ##拼音转汉字
        key_word_set_Han=set()
        for i in query_key_word_set:
            wordseg_revise=self.piny_han(i)
            key_word_set_Han.add(wordseg_revise)

        # 同义词转写改写
        # key_word_set_Han=self.query_text_transfer(key_word_set_Han)

        self.query_key_word_set=key_word_set_Han

    def query_text_transfer(self, key_word_set_Han):
        key_word_trans=set()
        for i in key_word_set_Han:
            if i in synonym_dict:
                syn_set=synonym_dict[i]
                key_word_trans.update(syn_set)
            else:
                key_word_trans.update({i})  ###注意：这里不能key_word_trans.update("你好")，而应该是key_word_trans.update({"你好"})

        return key_word_trans

    def piny_han(self, word):
        
        p_bool=True
        ##判断是否都是字母
        for c in word:
            if '\u4e00' <= c <= '\u9fa5':
                p_bool=False
                break
        
        if p_bool:
            word=word.lower()#转小写
            if word in self.wpy_dict:
                word_han=self.wpy_dict[word]
                a1 = sorted(word_han.items(),key = lambda x:x[1],reverse = True)
                wordseg_revise=a1[0][0]
                return wordseg_revise

        return word

    def named_entity_recognition(self):

        def ner_rewrite(word_ner):
            # 注意，只适用于important词汇
            # keys_important = word_ner.keys()
            # keys_important=[k for k, v in word_ner.items() if v=="schemas_important"]
            keys_important=word_ner['schemas_important']
            word_ner_re = {'schemas_important': [], 'schemas_not_important': [], 'schemas_words_join': []}
            for i in keys_important:
                keep_b = True
                for j in keys_important:
                    if i == j:
                        continue
                    if i in j:
                        keep_b = False
                if keep_b:
                    word_ner_re['schemas_important'].append(i)

            for i in word_ner['schemas_not_important']:
                word_ner_re['schemas_not_important'].append(i)

            for i in word_ner['schemas_words_join']:
                word_ner_re['schemas_words_join'].append(i)

            # for i, v in word_ner.items():
            #     if v=="schemas_not_important":
            #         word_ner_re[i] = v
            #     else:
            #         keep_b = True
            #         for j in keys_important:
            #             if i == j:
            #                 continue
            #             if i in j:
            #                 keep_b = False
            #         if keep_b:
            #             word_ner_re[i] = v

            return word_ner_re

        ##注意update的是一个集合{i},而不是字符串--query_schema[ner].update({i})
        
        # shopnameSetAll=set()#所有的店铺名字
        # query_schema={}#查询字段对应的命名实体
        # for i in self.query_key_word_set:#{'儿童','玩具'}
        #     if i in self.ner_dict:
        #         # query_ner[i]=ner_dict[i]#查询词query的实体--商品类别和简称
        #         ner_schema=self.ner_dict[i]#'玩具':{'goods_short_edit', 'spu_cate_third_edit'}
        #         for ner in ner_schema:
        #             if ner in query_schema:
        #                 query_schema[ner].update({i})#多个ner之间是or的关系--union是返回一个值，update是更新自身
        #             else:
        #                 query_schema[ner]={i}
        #     else:
        #         ner_schema='other_schema'
        #         if ner_schema in query_schema:
        #             query_schema[ner_schema].update({i})#多个ner之间是or的关系--union是返回一个值，update是更新自身
        #         else:
        #             query_schema[ner_schema]={i}
        #
        #
        #     shopnameSet=self.shopname_ner(i)
        #     if shopnameSet:
        #         shopnameSetAll.update(shopnameSet)
        #         if 'shop_name' in query_schema:
        #             query_schema['shop_name'].update(shopnameSet)#多个ner之间是or的关系
        #         else:
        #             query_schema['shop_name']=shopnameSet

        # schemas_important=['goods_short_edit', 'spu_cate_third_edit', 'goods_brand', 'goods_important']
        schemas_important = ['goods_short_edit', 'goods_brand', 'goods_important']  # 去掉类别词，比如搜索“牛奶饮品”，饮品貌似不重要！
        # query_ner_type={}  # 查询字段对应的命名实体的类别

        # for i in self.query_key_word_set:#{'儿童','玩具'}
        #     query_ner_type[i]='schemas_not_important'
        #     if i in self.ner_dict:
        #         ner_schema=self.ner_dict[i]#'玩具':{'goods_short_edit', 'spu_cate_third_edit'}
        #         for ner in ner_schema:
        #             if ner in schemas_important:
        #                 query_ner_type[i]='schemas_important'
        #
        # # print(query_ner_type)
        # query_ner_type=ner_rewrite(query_ner_type)

        query_ner_type = {'schemas_important': [], 'schemas_not_important': [], 'schemas_words_join': []}
        for i in self.query_key_word_set:  # {'儿童','玩具'}
            important_bool=False
            if i in self.ner_dict:
                ner_schema = self.ner_dict[i]  # '玩具':{'goods_short_edit', 'spu_cate_third_edit'}
                for ner in ner_schema:
                    if ner in schemas_important:
                        # query_ner_type[i] = 'schemas_important'
                        important_bool=True

            if important_bool:
                query_ner_type['schemas_important'].append(i)
            else:
                query_ner_type['schemas_not_important'].append(i)

        # print("tesss", query_ner_type)
        # 先判断是否存在了整体词
        # if (self.words_join not in query_ner_type['schemas_important']) and (self.words_join not in query_ner_type['schemas_not_important']):
        #     query_ner_type['schemas_words_join'].append(self.words_join)##把整体的词当成非重要词，解决“罐头酱菜”

        # 不加条件，把整体词加入，否则“火锅烧烤”是'schemas_not_important'，则整体词是空
        query_ner_type['schemas_words_join'].append(self.words_join)  # 把整体的词当成非重要词，解决“罐头酱菜”
        query_ner_type = ner_rewrite(query_ner_type)
        self.query_ner_type=query_ner_type
        # self.shopnameSetAll=shopnameSetAll
        # self.query_schema=query_schema

    def open_index_spu_search(self, arg_dict):

        def page_genertate(spu_code_search):

            rows_str = arg_dict['rows']
            page = arg_dict['page']

            rows = int(rows_str)
            goods_num = len(spu_code_search)
            page_size = int(np.ceil(goods_num / rows))  # rows=10##默认一页10个商品
            #    page_size=9
            #    spu_code_search={'vvd','afas'}
            # spu_code_search = list(spu_code_search)

            pageNum_int = int(page)  ##第几页，页码
            if pageNum_int == 1:
                isFirstPage = True
                hasPreviousPage = False
            else:
                isFirstPage = False
                hasPreviousPage = True

            if pageNum_int == page_size:
                isLastPage = True
                hasNextPage = False
            else:
                isLastPage = False
                hasNextPage = True

            ##暂时用page从1开始
            startRow = (pageNum_int - 1) * rows
            ##最后一页
            if isLastPage:
                endRow = goods_num
            else:
                endRow = pageNum_int * rows
            goods_code_page = spu_code_search[startRow:endRow]
            ##用join更简单
            spuCodes = ",".join(goods_code_page)
            data_dict = {
                #        "searchRult": "1",
                "searchRult": "1",
                "pageNum": pageNum_int,
                "pageSize": page_size,
                "size": 1,
                "startRow": startRow + 1,  # 从1开始
                "endRow": endRow,
                "total": 1,
                "pages": 1,
                "spuCodes": spuCodes,
                "prePage": 0,
                "nextPage": 0,
                "isFirstPage": isFirstPage,
                "isLastPage": isLastPage,
                "hasPreviousPage": hasPreviousPage,
                "hasNextPage": hasNextPage,
                "navigatePages": 8,
                "navigatepageNums": [1],
                "navigateFirstPage": 1,
                "navigateLastPage": 1,
                "firstPage": 1,
                "lastPage": 1}
            #    searchRult  1 完全匹配结果  2 不完全匹配结果 3 没有结果
            page_dict = {
                "resultCode": "0",
                "msg": "操作成功",
                "data": data_dict
                #               'testSpu':spu_code_search##测试分页的正确性
            }
            #    print(page_dict)
            # page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
            #    print(page_json)
            return page_dict

        def page_genertate_shop(spu_code_search):

            rows_str = arg_dict['rows']
            page = arg_dict['page']

            rows = int(rows_str)
            goods_num = len(spu_code_search)
            page_size = int(np.ceil(goods_num / rows))  # rows=10##默认一页10个商品
            #    page_size=9
            #    spu_code_search={'vvd','afas'}
            # spu_code_search = list(spu_code_search)

            pageNum_int = int(page)  ##第几页，页码
            if pageNum_int == 1:
                isFirstPage = True
                hasPreviousPage = False
            else:
                isFirstPage = False
                hasPreviousPage = True

            if pageNum_int == page_size:
                isLastPage = True
                hasNextPage = False
            else:
                isLastPage = False
                hasNextPage = True

            ##暂时用page从1开始
            startRow = (pageNum_int - 1) * rows
            ##最后一页
            if isLastPage:
                endRow = goods_num
            else:
                endRow = pageNum_int * rows
            goods_code_page = spu_code_search[startRow:endRow]
            data_dict = {
                "current": pageNum_int,
                "pages": page_size,
                "records": goods_code_page,
                "searchCount": True,
                "size": rows,
                "total": goods_num}
            page_dict = {
                "resultCode": "0",
                "msg": "操作成功",
                "data": data_dict
                #               'testSpu':spu_code_search##测试分页的正确性
            }
            # page_json = json.dumps(page_dict, ensure_ascii=False)  ##False解决中文乱码
            return page_dict

        def rankbyshop(shop_code_dict):

            # 用mysql根据销量和价格排序
            shop_code_list=list(shop_code_dict.keys())
            if len(shop_code_list) == 0:
                shop_group_list = []
                return shop_group_list

            if arg_dict['sort_method'] == '-1':
                # shop_group_list=rank_by_goods_scores(shop_code_dict)
                shop_group_list=[{k: v} for k, v in shop_code_dict.items()]
                return shop_group_list

            hot_list_str = ",".join(shop_code_list)
            mysqlCmdP="SELECT a.shop_code FROM %s AS a WHERE shop_code IN (%s)" % (self.cb_shop_rank_info, hot_list_str)

            if arg_dict['sort_method'] == '3' or arg_dict['sort_method'] == '4':  ##店铺评分从低到高
                mysqlCmd = mysqlCmdP + " ORDER BY a.shop_level"
            # elif sort_method == '4':
            #     mysqlCmd = mysqlCmdP + " ORDER BY a.shop_level DESC"
            elif arg_dict['sort_method'] == '1' or arg_dict['sort_method'] == '2':  ##销量从低到高
                mysqlCmd = mysqlCmdP + " ORDER BY a.shop_sales"
            # elif sort_method == '2':  ##销量从高到低
            #     mysqlCmd = mysqlCmdP + " ORDER BY a.shop_sales DESC"
            else:
                ##默认排序
                shop_group_list = [{k: v} for k, v in shop_code_dict.items()]
                return shop_group_list

            dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
            dataDf = pd.read_sql(mysqlCmd, dbc)
            shop_list_sort = dataDf['shop_code'].tolist()
            if arg_dict['sort_method'] == '4' or arg_dict['sort_method'] == '2':##避免相同的评分，顺序按照更新时间，不能完全相反
                shop_list_sort.reverse()
            dbc.close()

            shop_group_list=[{k:shop_code_dict[k]} for k in shop_list_sort]
            return shop_group_list

        def search_result_goods():

            def search_qp(qp):

                if arg_dict['sort_method'] == '-1':
                    results = sc_online.search(qp, limit=None, filter=filter)
                elif arg_dict['sort_method'] == '1':
                    results = sc_online.search(qp, limit=None, filter=filter, sortedby='sale_price', reverse=False)
                elif arg_dict['sort_method'] == '2':
                    results = sc_online.search(qp, limit=None, filter=filter, sortedby='sale_price', reverse=True)
                elif arg_dict['sort_method'] == '3':
                    results = sc_online.search(qp, limit=None, filter=filter, sortedby='sale_month_count',
                                               reverse=True)
                else:
                    results = sc_online.search(qp, limit=None, filter=filter)

                return results

            def query_one():

                if len(query_not_important) == 0:
                    qp = qparser.MultifieldParser(fieldnames=self.fieldnames, schema=self.schema_spu_search,
                                                  group=qparser.AndGroup).parse(query_important)
                    results = search_qp(qp)

                elif len(query_important) == 0:
                    qp = qparser.MultifieldParser(fieldnames=self.fieldnames, schema=self.schema_spu_search,
                                                  group=qparser.OrGroup).parse(query_all)
                    results = search_qp(qp)
                else:
                    qp = qparser.MultifieldParser(fieldnames=self.fieldnames, schema=self.schema_spu_search,
                                                  group=qparser.OrGroup).parse(query_all)
                    results_all = search_qp(qp)

                    if len(results_all) == 0:
                        results = []
                    else:
                        qp = qparser.MultifieldParser(fieldnames=self.fieldnames, schema=self.schema_spu_search,
                                                      group=qparser.AndGroup).parse(query_important)
                        results_filter = sc_online.search(qp, limit=None, scored=False, sortedby=False,
                                                          filter=filter)  # 不计分
                        spu_codes_list = [hit['spu_code'] for hit in results_filter]
                        results = [i for i in results_all if i['spu_code'] in spu_codes_list]
                        # results_all.filter(results)#也可以

                return results

            def query_cate():
                #  雀巢巧克力，进入了类别，所以这里应该用'schemas_words_join'
                #  擦，进而导致没有进入其他字段field的搜索！！！！即“绿茶味巧克力”无法搜到，只匹配了类别字段self.fieldnames_cate
                # query_all_join = " ".join(query_ner_type['schemas_not_important'] + query_ner_type['schemas_important'] + query_ner_type['schemas_words_join'])
                query_all_join = " ".join(query_ner_type['schemas_words_join'])
                qp = qparser.MultifieldParser(fieldnames=self.fieldnames_cate, schema=self.schema_spu_search,
                                              group=qparser.OrGroup).parse(query_all_join)
                results = search_qp(qp)
                return results

            if arg_dict['shop_code']=="-1":
                filter=None
            else:
                filter = Term("shop_code", arg_dict['shop_code'])

            # 类别搜索-火锅烧烤--如果没有结果，则继续搜索其他字段
            results = query_cate()
            if len(results)==0:
                results = query_one()

            # for hit in results:
            #     print(hit, hit.score, sep="\n")
            return results

        def get_page_goods():

            spu_codes_list_ori = [hit['spu_code'] for hit in results]
            # 去重
            # numbers = [1, 7, 3, 2, 5, 6, 2, 3, 4, 1, 5]
            spu_codes_list = list(set(spu_codes_list_ori))
            spu_codes_list.sort(key=spu_codes_list_ori.index)

            if len(spu_codes_list) == 0:
                page_json = page_json_default()
            else:
                page_json = page_genertate(spu_codes_list)

            return page_json

        def search_result_shops():
            # spu_codes_list = [hit['spu_code'] for hit in results]
            # print(spu_codes_list)
            # for hit in results:
            #     print(hit, hit.score, sep="\n")
            # print('number of results:', len(results))  # number of results: 3091
            if len(results) > 0:
                shop_code_dict = {}
                for hit in results:
                    # print('hit:\n', hit)
                    # print('score:\n', hit.score)
                    if hit['shop_code'] in shop_code_dict:
                        # 最多3个元素，并且去重
                        s_len = len(shop_code_dict[hit['shop_code']])
                        if (s_len >= 3) or (hit['spu_code'] in shop_code_dict[hit['shop_code']]):
                            continue
                        else:
                            shop_code_dict[hit['shop_code']].append(hit['spu_code'])
                    else:
                        shop_code_dict[hit['shop_code']] = [hit['spu_code']]
                shop_group_list = rankbyshop(shop_code_dict)
                # print(shop_group_list)
            else:
                shop_group_list=[]

            if len(shop_group_list) == 0:
                page_json = page_json_default_shop()
            else:
                page_json = page_genertate_shop(shop_group_list)

            return page_json
            # results = search_i_filter(query_all, query_important, sc_online)

        query_ner_type=arg_dict['query_ner_type']
        # query_all = "苹果 苹果啊 漂亮"
        # 信我家智慧社区虚拟社区的商品是全网可见的。。。
        # area_code = 2020032600001
        area_code=arg_dict['areaCode']
        # if area_code=='2020032600001':
        #     area_code='-1'
        # shop_code = arg_dict['shop_code']  #店铺编码的过滤！filterwhoosh---402324138263052288---只过滤而不参与评分score？
        # sort_method=arg_dict['sort_method']
        search_dim=arg_dict['search_dim']
        path_index=self.path_index_spu_search+"_"+area_code
        if not os.path.exists(path_index):
            if search_dim == '1':
                page_json=page_json_default()
            elif search_dim == '2':
                page_json=page_json_default_shop()
            else:
                page_json = ""
            return page_json
        else:
            sc_online = open_dir(path_index).searcher(weighting=self.pos_weighting)
        # sc_online = open_dir(path_index).searcher(weighting=pos_weighting_not_important)
        # qp = qparser.MultifieldParser(fieldnames=["goods_short_edit"], schema=self.schema_spu_search, group=qparser.OrGroup)
        # mparser=qparser.MultifieldParser(fieldnames=["title", "content"], schema=schema, group=qparser.AndGroup)
        # query_important = "苹果 苹果啊"
        query_not_important = " ".join(query_ner_type['schemas_not_important'])
        query_important = " ".join(query_ner_type['schemas_important'])
        query_all = " ".join(query_ner_type['schemas_not_important']+query_ner_type['schemas_important'])
        query_words_join=" ".join(query_ner_type['schemas_words_join'])
        # print(query_all+":all", query_important+":important", query_not_important+":not_important", query_words_join+":words_join", sep="\n")

        results = search_result_goods()
        if search_dim == '1':
            # page_json = search_result_goods(query_important, query_not_important, query_all, sc_online, arg_dict)
            page_json = get_page_goods()
        elif search_dim == '2':
            # page_json = search_result_shops(query_important, query_not_important, query_all, sc_online, arg_dict)
            page_json = search_result_shops()
        else:
            page_json=""

        # shop_code_dict=results.groups()
        # shop_code_dict = results.groups("shop_code")
        # print(type(shop_code_dict), shop_code_dict)
        # print(list(shop_code_dict.keys()), list(shop_code_dict.values()), sep="\n")

        # 本身返回的是一个字典，没有顺序。可以取出第一个记录，得到排序score，然后按照score排序分组
        # 可能是Python的字典的默认排序规则--自己测试一下字典的排序，应该是按照创建key的顺序排序的
        # 如果score相同，再按照匹配数量？
        # --好像是按照数据的更新时间排序的，最新的数据对应的分组靠前--等价于文档编号的顺序doclist！！！
        # 最终证明：有两种排序，
        # 分组的排序是按照首个编号的大小，即修改更新时间
        # 组内的排序默认按照score排序的，只是在分数score相同的情况下，才按照修改时间排序

        """
        返回商铺列表之后，要进行再次排序。
        1、按照销量--搜索另一个“商铺-销量”whoosh索引？暂时用mysql，速度还行
        先把商品列表的结果放到redis里面？失效期是多长时间？
        先从redis取，如果有的话直接调用商铺排序接口，否则重新搜索一遍！
        2、按照自定义的方法--组内商品相关性score排序
        """
        sc_online.close()
        return page_json


##请求修正
class QueryRevise:
    
    def __init__(self, wordFreq):

        pathDir = os.path.dirname(__file__)
        path_wpy_dict = os.path.join(pathDir, 'word_pkls', 'wpy_dict.pkl')
        # path_whz_dict = os.path.join(pathDir, 'word_pkls', 'whz_dict.pkl')
        path_ner_dict = os.path.join(pathDir, 'word_pkls', 'name_entity_dict.pkl')
        # path_goods_cate = os.path.join(pathDir, 'word_pkls', 'goods_cate_set.pkl')

        self.wpy_dict=pickle.load(open(path_wpy_dict, 'rb'))
        self.name_entity_dict=pickle.load(open(path_ner_dict, 'rb'))
        self.wordFreq=wordFreq

    #  先把全词进行映射，如果能找到汉字，则输出汉字，否则再分词之后，转拼音，转汉字。防止分成单字
    def seg_and_change(self, query):

        # 在剔除停用词之前，先剔除语音搜索专有词库，再判断单字
        # 对于单字，映射到电商词典，不用停用词过滤，
        query=query.replace('。', '')  # 删除最后一个句号
        query_seg=list(jieba.cut(query, cut_all=False))  # 精准分词
        # print('正式环境：',query_seg)
        single_word=""
        for word_han in query_seg:
            if word_han not in yy_stopwords:##语音yy电商停用词专用--不是所有的停用词！
                # single_word.append(word_han)
                single_word += word_han #字符串

        #  先把全词进行映射，如果能找到汉字，则输出汉字，否则再分词之后，转拼音，转汉字。防止分成单字
        # 先全词，如果找不到汉字，则去掉停用词之后再全词？

        """先判断分词是否在词库"""
        self.qBool=True

        if single_word in self.name_entity_dict:
            self.qBool=False #不用往下进行了
            wordhan_revise=single_word
            # print('正式2:', single_word)
        else:
            word_piny=self.changeto_pinyin(single_word)
            # print('正式1:',word_piny)
            if (word_piny in self.wpy_dict):
                self.qBool=False #不用往下进行了
                wordhan_revise=self.wordseg_choose_for_tf(word_piny, single_word)

        if not self.qBool:
            # query_p_r=''
            # for i in query_seg:
            #     if i in yy_stopwords:
            #         query_p_r += i
            # query_p_r += wordhan #把整体的词放到最后面--可能会有语句还原不够完整
            query_p_r="".join(query_seg).replace(single_word, wordhan_revise)
            self.query_p_r=query_p_r
        else:
            #  先把全词进行映射，如果能找到汉字，则输出汉字，否则再分词之后，转拼音，转汉字。防止分成单字
            query_pinyin=[]
            for word_han in query_seg:
                query_pinyin.append({'word_pin':self.changeto_pinyin(word_han), 'word_han':word_han})
            self.qBool=True #往下进行
            self.query_pinyin=query_pinyin

    def changeto_pinyin(self, word):
        s = ''
        for i in pypinyin.pinyin(word, style=pypinyin.NORMAL):
            s += ''.join(i)
        return s
    
    def query_pinyin_revise(self):
        query_p_r=''
        for word_ph in self.query_pinyin:#拼音，汉字的元组
            
            ##停用词“的”不修正
            i_revise=self.seg_pinyin_revise(word_ph)#单词分词修正
            query_p_r += i_revise
        self.query_p_r=query_p_r
    
    def seg_pinyin_revise(self, word_ph):#单词分词修正
        
        """先判断分词是否在词库"""
        
        if word_ph['word_han'] in self.name_entity_dict:
            return word_ph['word_han']
        
        if (word_ph['word_pin'] in self.wpy_dict) and (word_ph['word_han'] not in stopwords):#的--德
            word_han_ori=word_ph['word_han']
            wordseg_revise=self.wordseg_choose_for_tf(word_ph['word_pin'], word_han_ori)
        else:
            wordseg_revise=word_ph['word_han']
        return wordseg_revise
    
    
    ##选出电商字典中频率最高的词--注意有两个地方用到了这个方法
    def wordseg_choose_for_tf(self, word_pin, word_han_ori):
        word_han=self.wpy_dict[word_pin]
        a1 = sorted(word_han.items(), key=lambda x: x[1], reverse=True)
        if a1[0][1]>self.wordFreq:  # 加入词频判断
            wordseg_revise=a1[0][0]
        else:
            wordseg_revise=word_han_ori  # 原始汉字

        return wordseg_revise
        
    ##可以用whoosh做模糊匹配
    ##下一步编辑距离=1的过滤
    ##key-value，文件，读取到内存中
    
# 商铺过滤

# def shopFilter():
#     mysqlCmd="SELECT gss.shop_name FROM bd_goods_spu_search as gss GROUP BY shop_name;"

#     dbc = pymysql.connect(host=ip,user=user,password=password,db=db,port=port)
#     dataDf=pd.read_sql(mysqlCmd, dbc)
#     # print(dataDf.columns)
#     # 关闭数据库连接
#     dbc.close()
#     return dataDf


sge=SearchGoodsEgine()##这里初始化的都是一些可变的配置，比如路径，比如算法的阈值设置。不能硬编码hardcode
# 耗时的操作，或者只运行一次的变量操作，放到内存里面！！！
# （甚至mysql数据库都可以只初始化一次？）
# 1、mysql如果长连接而长期没有对数据库进行任何操作，在timeout值后，
# mysql server就会关闭此链接，程序执行查询的时候就会得到一个类似于“MYSQL server has gone away”这样的错误。

# wordFreq=30#语义纠正词库的词频阈值参数设置！！！
wordFreq=5#佳沛
qr=QueryRevise(wordFreq)  # 参数初始化！！！软编码softcode

if __name__=='__main__':
    # pass
    for query in ["白啤", "白酒", "白酒白啤", "琪贝斯"]:
        seg_list = list(jieba.cut_for_search(query))
        words = jieba.lcut_for_search(query, HMM=False)
        print(seg_list, words, sep="\n")

    # arg_dict={'query':"白酒白啤", 'areaCode':'-1', 'sort_method':'0'
    #             , 'search_dim':'2', 'rows':'10', 'page':'1'}
    # from searchmatch.search_for_xwj import sge
    # sge.keyword_extract(arg_dict)
