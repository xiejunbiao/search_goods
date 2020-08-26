# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 18:34:08 2020

@author: lijiangman
"""
import os, pickle
import jieba
from whoosh import qparser, scoring
from whoosh.analysis import StandardAnalyzer
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema,TEXT,ID,NUMERIC
from jieba.analyse import ChineseAnalyzer
from whoosh.analysis import NgramTokenizer
from whoosh.query import Every
from whoosh.writing import AsyncWriter

# jieba.load_userdict("/opt/ado-services/bigdata-search/bigdata-search/searchMatchV2/utils/words_brand_goods_all.txt")

# 项目路径
# pathDir = os.path.realpath(__file__)
# config.ini文件路径
# pathDir=os.getcwd()
# pathDir=os.path.realpath(__file__)

# pathDir=os.path.dirname(__file__)
# configFilePath = os.path.join(pathDir, 'searchMatchV2','utils','words_brand_goods_all.txt')
# print(configFilePath)
# print(pathDir)
# jieba.load_userdict(configFilePath)

# from searchMatchV2.search_for_xwj import search_main #模块名字不能有小数点
# print('aaa')

def search_online_test():
    schema=Schema(goods_short = TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                           goods_brand = TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)),
                           spu_code = ID(stored=True, unique=True),##spu_code是唯一值，用来update索引
                           # area_code = ID(stored=True, unique=False),##ID不用分词---应该分小区！否则数据量太大
                           search_frequency = NUMERIC(numtype=int, stored=True, sortable=True))##搜索频次


    sc_hot=open_dir("./index_A2018012300015").searcher()
    # results_hot_list=list(sc_hot.lexicon("goods_short"))
    
    myquery = Every()
    results=sc_hot.search(myquery, limit=50, sortedby='search_frequency', reverse=True)#limit=None返回所有结果
    # results_hot_list=[i['goods_short'] for i in results_hot]
    
    print(len(results))
    if len(results)>0:
        for hit in results:
            print('hit:\n', hit)
            # print('content:\n', hit['content'])
            print('score:\n', hit.score)
    else:
        print('empty')

    # results_online_list=[str(i, encoding = "utf-8") for i in sc_online.lexicon("goods_short")]
    sc_hot.close()## 记得close啊sc_hot。close
    
    
    return results

if __name__=="__main__":
    
    
    search_online_test()
    
    
    
    
    