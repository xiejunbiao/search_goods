#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 13:07:04 2020

@author: lijiangman
"""
# import whoosh
from whoosh.query import Every
from whoosh.index import open_dir, create_in
from whoosh import qparser, matching
from whoosh import highlight
from whoosh.query import FuzzyTerm
from whoosh.query import Wildcard, And, Term, MultiTerm, PatternQuery
from whoosh import scoring
from whoosh.fields import Schema,TEXT,ID
from whoosh.analysis import StandardAnalyzer
# from jieba.analyse import ChineseAnalyzer
# import jieba

from searchmatch.analyzerbyme import ChineseAnalyzer, ChineseAnalyzerMerge
from searchmatch.analyzerbyme import ChineseTokenizer_Merge_Byme
import heapq
from itertools import chain


import time,os
from operator import itemgetter
from searchhotmain.inition_db import SelectMysqlDatabase
import pymysql
import pandas as pd
from whoosh.analysis import NgramTokenizer

# from bs4 import BeautifulSoup
# from django.utils.html import strip_tags 

## def format_token(self, text, token, replace=False):
# Python编程中raise可以实现报出错误的功能，而报错的条件可以由程序员自己去定制。
# 在面向对象编程中，可以先预留一个方法接口不实现，在其子类中实现。
# 如果要求其子类一定要实现，不实现的时候会导致问题，那么采用raise的方式就很好。

##这个把字段全部内容输出了
class BracketFormatter(highlight.Formatter):
    
    """Puts square brackets around the matched terms.
    """
    def format_token(self, text, token, replace=False):
        # Use the get_text function to get the text corresponding to the
        # token
        tokentext = highlight.get_text(text, token, replace)

        # Return the text as you want it to appear in the highlighted
        # string
        # return "[%s]" % tokentext
        return tokentext

##可以解决哈闪洗发水？模糊匹配
class MyFuzzyTerm(FuzzyTerm):
    
     def __init__(self, fieldname, 
                  text, boost=1.0, maxdist=5, 
                  prefixlength=1, constantscore=True):
         super(MyFuzzyTerm, self).__init__(fieldname, 
                                           text, boost, 
                                           maxdist, 
                                           prefixlength, 
                                           constantscore)
         
def pos_score_fn(searcher, fieldname, text, matcher):
    
    if fieldname=='content':
        score=1.0
    elif fieldname=='title':
        score=0.1
    else:
        score=1
        
        ##加湿 加湿机 一共命中title2次，因此，score=2*2
        ##深圳市命中content1次，因此score=1
        ##总分score=4+1=5
    return score

def len_score_fn(searcher, fieldname, text, matcher):
    
    """
    命中词的长度
    """
    score=1.0/len(text)
        
    return score


def schemavalue_score_fn(searcher, fieldname, text, matcher):
    """
    根据词频字段，作为分数？？
    """
    pass
    # return score

class SearchingGoods:
    
    def __init__(self, indexdir, index_hot, index_online, brf, my_cf):

        
        self.nSum=2
        self.indexdir=indexdir
        self.index_hot=index_hot
        self.index_online=index_online
        # self.ix=open_dir(self.indexdir)
        # self.qp = qparser.QueryParser("content", 
                                      # schema=self.ix.schema,
                                      # termclass=MyFuzzyTerm)
        
        
        # self.queryWild=Wildcard('content',query)

        ##只打开一次searcher，然后一直调用？？？不用close？？？？
        self.hi=highlight.Highlighter()
        self.hi.formatter = brf
        self.hi.fragmenter=my_cf
        
        
        self.pos_weighting = scoring.FunctionWeighting(pos_score_fn)
        self.len_weighting = scoring.FunctionWeighting(len_score_fn)

        self.schema=Schema(title=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                      ,goods_short=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)))
                      

        # self.ix=open_dir(self.indexdir)
        
        # self.searcher=self.ix.searcher(weighting=scoring.BM25F)
        # self.searcher.search(myquery).formatter=brf
        # self.searcher.search(myquery).fragmenter=my_cf
        
        # self.searcher.search.formatter=brf
        # a=self.searcher.search()
        # from whoosh.searching import Results
        # Results.formatter=brf
        # Results.fragmenter=my_cf
        # self.searcher=self.ix.searcher(weighting=scoring.BM25F).search
        # searching.formatter=brf

        # self.queryparser=QueryParser('content', schema=self.ix.schema)
        # query = QueryParser('content', schema=ix.schema)
        # 下面两行表示可以使用通配符搜索，如”窗前*月光“
        # query.remove_plugin_class(qparser.WildcardPlugin)
        # query.add_plugin(qparser.PrefixPlugin())    
        # q = query.parse(u'%s' % word)

##分词
    def searchforgoods_seg(self, query):
        
        ##对请求query进行分词--自带的英文分词，对中文只能是整体一个词
        # schema=Schema(title=TEXT(stored=True,analyzer=ChineseAnalyzer())
        #               ,dynasty=ID(stored=True)
        #               ,poet=ID(stored=True)
        #               ,content=TEXT(stored=True, analyzer=ChineseAnalyzer(minsize=1)))

        
        schema=Schema(title=TEXT(stored=True,analyzer=ChineseAnalyzer())
                      ,dynasty=ID(stored=True)
                      ,poet=ID(stored=True)
                      ,content=TEXT(stored=True, analyzer=ChineseAnalyzer()))
        
        
        # schema=Schema(title=TEXT(stored=True, analyzer=whoosh.analysis.StandardAnalyzer(minsize=1))
        #               ,dynasty=ID(stored=True)
        #               ,poet=ID(stored=True)
        #               ,content=TEXT(stored=True, analyzer=ChineseAnalyzer()))
                              
        ##请求query和数据库都分词，然后匹配
        ##数据库如果用自带的英文分词，则不会分开“深圳市”
        # qp=qparser.QueryParser(fieldname="content"
        #                        ,schema=ix.schema
        #                        ,group=qparser.OrGroup)
        # qp=qparser.QueryParser(fieldname="content"
                               # ,schema=schema
                               # ,group=qparser.OrGroup)
        
        qp=qparser.MultifieldParser(fieldnames=["title"]
                               ,schema=schema)
        myquery=qp.parse(query)
        print('seg:',myquery)
        
    def searchforgoods_bm(self, query):
        
        ##对请求query进行分词--自带的英文分词，对中文只能是整体一个词
        # schema=Schema(title=TEXT(stored=True,analyzer=ChineseAnalyzer())
        #               ,dynasty=ID(stored=True)
        #               ,poet=ID(stored=True)
        #               ,content=TEXT(stored=True, analyzer=ChineseAnalyzer(minsize=1)))

        
        # schema=Schema(title=TEXT(stored=True,analyzer=ChineseAnalyzer())
        #               ,dynasty=ID(stored=True)
        #               ,poet=ID(stored=True)
        #               ,content=TEXT(stored=True, analyzer=ChineseAnalyzer()))
        
        ##按照空格隔开分词，采用英文分词
        schema=Schema(title=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                      ,dynasty=ID(stored=True)
                      ,poet=ID(stored=True)
                      ,content=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)))
                              
        ix=open_dir(self.indexdir)
        ##请求query和数据库都分词，然后匹配
        ##数据库如果用自带的英文分词，则不会分开“深圳市”
        # qp=qparser.QueryParser(fieldname="content"
        #                        ,schema=ix.schema
        #                        ,group=qparser.OrGroup)
        # qp=qparser.QueryParser(fieldname="content"
                               # ,schema=schema
                               # ,group=qparser.OrGroup)
        
        # qp=qparser.MultifieldParser(fieldnames=["title", "content"]
        #                        ,schema=schema
        #                        ,group=qparser.OrGroup)
        mparser=qparser.MultifieldParser(fieldnames=["title", "content"], schema=schema, group=qparser.AndGroup)
        myquery=mparser.parse(query)
        # myquery=qp.parse(query)
#        mw = scoring.MultiWeighting(scoring.BM25F()
#                                    ,content=scoring.Frequency()
#                                    ,title=scoring.TF_IDF())
#        mw = scoring.MultiWeighting(scoring.BM25F()
#                                    ,content=scoring.Frequency()
#                                    ,title=scoring.TF_IDF())

#        with ix.searcher(weighting=scoring.BM25F) as sc:
        
        # pos_weighting = scoring.FunctionWeighting(pos_score_fn)
        with ix.searcher(weighting=self.pos_weighting) as sc:
            
#        with ix.searcher(weighting=scoring.BM25F(content_B=1.0)) as sc:
        # with ix.searcher(weighting=mw) as sc:
            print('---------search_match:\nmyquery:',myquery)
            # 每页20个，返回第5页
            # results = sc.search_page(myquery, 5, pagelen=20)
            # print('page:')
            # for i in results:
                # print(i)
            results=sc.search(myquery, limit=None)#limit=None返回所有结果
            # mf = sorting.MultiFacet([sorting.FieldFacet("content")
            #                          ,sorting.ScoreFacet()])
#            mf = sorting.MultiFacet([sorting.FieldFacet("content")])
#            results = sc.search(myquery, sortedby=mf)
#            tags = sorting.FieldFacet("content")
#            results = sc.search(myquery, groupedby=tags)

            if len(results)>0:
                for hit in results:
                    print('hit:\n', hit)
                    print('content:\n', hit['content'])
                    print('score:\n', hit.score)
            else:
                print('empty')
            
        return results
    
    def search_lianxiang(self, query):
        
        ##对于content里面的空格，分开的，正好满足“尿不湿”和“纸尿裤”搜索联想
        
        # myquery = Wildcard('content', query)# ??content or title
        
        # schemaLx='title'#联想的字段
        schemaLx='goods_short'#联想的字段--适用于多个分词，空格隔开
        schema_spu_code='spu_code'
        # >>> Term("content", u"a") & Term("content", u"b")
        # And([Term("content", u"a"), Term("content", u"b")])
        # PatternQuery=And([Term(schemaLx, query), Term(schema_spu_code, '367517646367543297')])
        # PQuery=Term(schemaLx, query) & Term(schema_spu_code, '367517646367543297')
        query_object = Wildcard(schemaLx, query)
        myquery=query_object
        # myquery = And([query_object, Term(schema_spu_code, '367517646367543297')])
        ix=open_dir(self.indexdir)

        # 如何让“米”的结果靠前？打分模型，字数？
        # myquery.matcher().value_as("positions")
        
        with ix.searcher(weighting=self.len_weighting) as sc:

        # with ix.searcher(weighting=scoring.BM25F) as sc:
            # myquery=self.parser.parse(self.query)##完全匹配
            print('myquery:',myquery)
            # results=self.searcher.search(myquery, limit=None)#默认是10
            # results.formatter = brf
            # results.fragmenter=my_cf
            results=sc.search(myquery, limit=None)#默认是10
            # hit_set=set()
            hit_list=[]
            hit_list_uni=[]
            if len(results)>0:
                for hit in results:
                    print(hit.fields)
                    # print(schemaLx, ':', hit[schemaLx])
                    hit_seg=self.hi.highlight_hit(hit, schemaLx, top=10)
                    # print(hit_seg)
                    """
                    没有多个词的时候，不用split
                    """
                    hit_list.append(hit_seg)
                    # for j in hit_seg.split('...'):
                        # hit_set.add(j)
                        # hit_list.append(j)
                        
                    # print('score:\n', hit.score)
                
                hit_list_uni=list(set(hit_list))
                hit_list_uni.sort(key=hit_list.index)

            else:
                pass
            
            
        # return hit_set
        return hit_list_uni
    
    def search_lianxiang_two(self, query):
        
        ##对于content里面的空格，分开的，正好满足“尿不湿”和“纸尿裤”搜索联想
        
        # myquery = Wildcard('content', query)# ??content or title
        
        # schemaLx='title'#联想的字段
        schemaLx='goods_short'#联想的字段--适用于多个分词，空格隔开
        schema_spu_code='spu_code'
        # >>> Term("content", u"a") & Term("content", u"b")
        # And([Term("content", u"a"), Term("content", u"b")])
        # PatternQuery=And([Term(schemaLx, query), Term(schema_spu_code, '367517646367543297')])
        # PQuery=Term(schemaLx, query) & Term(schema_spu_code, '367517646367543297')
        # query_object = Wildcard(schemaLx, query)
        qp=qparser.MultifieldParser(fieldnames=["goods_short"]
                           ,schema=self.schema)
        myquery=qp.parse(query)
        # myquery = And([query_object, Term(schema_spu_code, '367517646367543297')])
        # ix=open_dir(self.indexdir)

        # 如何让“米”的结果靠前？打分模型，字数？
        # myquery.matcher().value_as("positions")
        
        with self.ix.searcher(weighting=self.len_weighting) as sc:

        # with ix.searcher(weighting=scoring.BM25F) as sc:
            # myquery=self.parser.parse(self.query)##完全匹配
            print('myquery:',myquery)
            # results=self.searcher.search(myquery, limit=None)#默认是10
            # results.formatter = brf
            # results.fragmenter=my_cf
            results=sc.search(myquery, limit=None)#默认是10
            # hit_set=set()
            hit_list=[]
            hit_list_uni=[]
            if len(results)>0:
                for hit in results:
                    print(hit.fields)
                    # print(schemaLx, ':', hit[schemaLx])
                    # hit_seg=self.hi.highlight_hit(hit, schemaLx, top=10)
                    # print(hit_seg)
                    """
                    没有多个词的时候，不用split
                    """
                    # hit_list.append(hit_seg)
                    hit_list.append(hit[schemaLx])##速度较快
                    # for j in hit_seg.split('...'):
                        # hit_set.add(j)
                        # hit_list.append(j)
                        
                    # print('score:\n', hit.score)
                
                hit_list_uni=list(set(hit_list))
                hit_list_uni.sort(key=hit_list.index)

            else:
                pass
            
            
        # return hit_set
        return hit_list_uni
    
    
    def get_suggestions(self, prefix, area_code):
        
        if prefix=='':
            return ''

        path_index=self.indexdir+"_"+area_code
        # print(path_index)
        ix=open_dir(path_index)
        """
        能否用户输入的时候，重复调用输入prefix而不退出？直到用户点击搜索？
        yield迭代器？return的区别？
        """
        # with self.ix.reader() as r:
        with ix.reader() as r:
            
            """
            商品名字段，麻辣香锅也要索引，用空格分开？yes

            """
            suggest_goods_short=r.iter_prefix('goods_short', prefix)
            suggest_goods_brand=r.iter_prefix('goods_brand', prefix)
            # print(type(suggest_goods_brand))

            # suggestions = [(s[0].decode(), s[1].doc_frequency()) for s in r.iter_prefix(fieldname, prefix)]
            # suggestions = [s for s in r.iter_prefix(fieldname, prefix)]
            suggestions_goods_short = [(s[0].decode(), 1.0/len(s[0])) for s in suggest_goods_short]
            suggestions_goods_brand = [(s[0].decode(), 1.0/len(s[0])) for s in suggest_goods_brand]
            suggestions=suggestions_goods_brand+suggestions_goods_short
            if not suggestions:
                return ''##空
            # print(len(suggestions))##suggestion已经去重
            suggestions_sort=sorted(suggestions, key=itemgetter(1), reverse=True)
            suggestions_sort_zip=list(zip(*suggestions_sort))
            # print(type(suggestions_sort_zip))
        
        return suggestions_sort_zip[0]
        # return suggestions
    
    def seg_word(self, area_code):
        
        # ix=open_dir(self.indexdir)
        # path_index=self.indexdir+"_"+area_code
        index_hot=self.index_hot+"_"+area_code
        index_online=self.index_online+"_"+area_code
        ix=open_dir(index_online)

        """
        索引分词字典text_dict
        已经去重
        """
        text_dict=set()
        with ix.reader() as ir:
            segword=ir.lexicon('goods_short')
            segword_len=0
            for i in segword:
                segword_len +=1
                text_dict.add(i.decode())
                # print('----\n', i.decode())
            # print(segword_len)
            # print(len(text_dict))

        self.text_dict=text_dict
   
    def get_hot_search(self, key_area_code, area_code):
        """
        获取热搜序列
        key_spu_code，键
        area_code，小区索引
        """

        index_hot=self.index_hot+"_"+area_code
        ix_hot=open_dir(index_hot)
        index_online=self.index_online+"_"+area_code
        ix_online=open_dir(index_online)

        schema=Schema(title=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1))
                      ,area_code=ID(stored=True)
                      ,poet=ID(stored=True)
                      ,content=TEXT(stored=True, analyzer=StandardAnalyzer(minsize=1)))

     
        mparser=qparser.MultifieldParser(fieldnames=["area_code"], schema=schema, group=qparser.AndGroup)
        myquery=mparser.parse(key_area_code)
        # myquery=qp.parse(query)
        # results_hot=self.get_result_of_query(ix_hot, myquery)
        # print(results_hot)
        # results_online=self.get_result_of_query(ix_online, myquery)
        # results_hot.filter(results_online)
        results_hot=self.result_filter_online(ix_hot, ix_online, myquery)
        # print(results_hot)
        # for i in results_hot:
            # print(i)

        return results_hot
    
    def result_filter_online(self, ix_hot, ix_online, myquery):
        """
        商品在线状态过滤
        """
        # print('---------search_match:\nmyquery:',myquery)
        # with ix.searcher() as sc:
        sc_hot=ix_hot.searcher()
        results_hot=sc_hot.search(myquery, limit=None, sortedby='search_frequency', reverse=True)#limit=None返回所有结果
        sc_online=ix_online.searcher()
        results_online=sc_online.search(myquery, limit=None)
        # results_online=sc_online.search(myquery, limit=None, groupedby=["goods_short", "goods_brand"])#limit=None返回所有结果
        # results_online.groups("goods_short")
        # print(results_online.fields(0))
        
        # 此对象由搜索者返回。此对象表示搜索查询的结果。您可以把它当作字典列表来使用，
        # 其中每个字典都是文档在结果中该位置的存储字段。
        # 请注意，结果对象保持对创建它的搜索者的引用，因此保持对结果对象的引用将保持搜索者的活动状态，
        # 并保持它使用的所有文件都处于打开状态。
        
        # print(results_online)
        # for i in results_online:
            # print(i)
        # print(results_online.docs())

        # results_hot.filter(results_online)####是按照序号过滤的，因此只适用于同一个es数据库的搜索结果result
        # items = [item for item in self.top_n if item[1] in otherdocs]

        results_hot_list=[i['goods_short'] for i in results_hot]
        results_online_list=[i['goods_short'] for i in results_online]
        results_hot_filter=[i for i in results_hot_list if i in results_online_list]
        
        results_hot_filter=[]
        n=0
        
        for i in results_hot_list:
            
            if i in results_online_list:
                n += 1
                
                if n>self.nSum:
                    break
                else:
                    results_hot_filter.append(i)    
        """
        热搜索引是离线计算的，预留足够的数量1000，不需要更新。每次调用，取前10个，至少取10个

        """
        if len(results_hot_filter)<self.nSum:
            results_hot_filter=results_hot_list[:self.nSum]
            print('less than 10')
            
        # b3=list(set(b1) & set(b2))##没有顺序

        # results_online_set=map(lambda x:x.get('goods_short'), results_online)
        # for i in results_hot:
            # if i[] in results_online
            # print(i)
        
        
        sc_hot.close()
        sc_online.close()
        # print(type(results))
            # print(results[0])

            # if len(results)>0:
            #     for hit in results:
            #         # print('hit:\n', hit)
            #         print('area_code:\n', hit['area_code'])
            #         print('search_frequency:\n', hit['search_frequency'])
            #         # print('score:\n', hit.score)
            #         print('goods_short:\n', hit['goods_short'])
            # else:
            #     print('empty')
        
        return results_hot_filter
    
    def get_result_of_query(self, ix, myquery):
        
        # pos_weighting = scoring.FunctionWeighting(pos_score_fn)
        # with ix.searcher(weighting=self.pos_weighting) as sc:
        with ix.searcher() as sc:
            print('---------search_match:\nmyquery:',myquery)
            results=sc.search(myquery, limit=None, sortedby='search_frequency', reverse=True)#limit=None返回所有结果
            # print(type(results))
            # print(results[0])

            # if len(results)>0:
            #     for hit in results:
            #         # print('hit:\n', hit)
            #         print('area_code:\n', hit['area_code'])
            #         print('search_frequency:\n', hit['search_frequency'])
            #         # print('score:\n', hit.score)
            #         print('goods_short:\n', hit['goods_short'])
            # else:
            #     print('empty')
        
        return results
        
        

def search_asso_test():
    
    """
    搜索联想测试
    """
    
    ##把读取index放到初始化，加速

    querysLianx=['洗','洗发', '皂', '米', '够']
    querysLianx=['海','洗','']
    brf = BracketFormatter()
    my_cf = highlight.ContextFragmenter(maxchars=100, surround=0)
    pathDir=os.path.dirname(__file__)
    path_index = os.path.join(pathDir, 'index_asso_all','index_asso')
    sg=SearchingGoods(indexdir=path_index, brf=brf, my_cf=my_cf)

    area_code='A2018012300015'
    area_code='-1'
    # area_code='A2018012300005'
    sg.seg_word(area_code)
    text_dict=sg.text_dict

    start = time.time()
    for i in querysLianx:
        # query=i+"*"
        # hit_set=sg.search_lianxiang(query=query)
        # hit_set=sg.search_lianxiang_two(query=query)
        hit_set=sg.get_suggestions(prefix=i, area_code=area_code)

        print('hit_set:', hit_set, sep='\n')

    
    elapsed = time.time() - start
    print("Time used:%s s" %(elapsed))    # 时间长？？60ms

def search_hot_whoosh_test():
    
    """
    搜索热搜测试
    """
    brf = BracketFormatter()
    my_cf = highlight.ContextFragmenter(maxchars=100, surround=0)
    pathDir=os.path.dirname(__file__)
    index_hot = os.path.join(pathDir, 'index_hot_all','index_hot')
    index_online = os.path.join(pathDir, 'index_online_all','index_online')
    sg=SearchingGoods(indexdir=index_hot, index_hot=index_hot, index_online=index_online, brf=brf, my_cf=my_cf)

    area_code='-1'
    sg.seg_word(area_code)
    text_dict=sg.text_dict
    print('len of text_dict:', len(text_dict))

    start = time.time()
    key_area_code='001'
    results_hot=sg.get_hot_search(key_area_code=key_area_code, area_code=area_code)
    """
    
    results_hot可以进一步存成一个过滤后的索引
    """
    # print('hit_set:', hit_set, sep='\n')
    
    elapsed = time.time() - start
    print("Time used:%s s" %(elapsed))    # 时间长？？60ms
    return results_hot

def search_hot_mysql_test():
    
    """
    搜索热搜测试
    """
    smd=SelectMysqlDatabase()

    area_code='A2018012300015'
    # area_code='-1'
    # area_code='A2018012300005'

    start = time.time()
    key_spu_code='001'
    # hit_set=sg.get_hot_search(key_spu_code=key_spu_code, area_code=area_code)

    """
        读取商品简称和品牌
    """
    dbc = pymysql.connect(host=smd.ip,user=smd.user,password=smd.password,db=smd.db,port=smd.port)

   
    a_col="a.spu_code, a.spu_name, a.goods_short_edit\
        ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
        ,a.spu_cate_third, a.spu_cate_third_edit\
        ,a.sale_price, a.sale_month_count, a.shop_name"    
    
    if area_code=='-1':
        mysqlCmd="SELECT %s FROM %s a  \
                    where a.goods_status=1 " % (a_col, smd.gss_tb)
    else:
        mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                    and b.area_code='%s' \
                    and a.goods_status=1 " % (a_col, smd.gss_tb, smd.gs_tb, area_code)
    
    dataDf=pd.read_sql(mysqlCmd, dbc)
    dbc.close()
    
    elapsed = time.time() - start
    print("Time used:%s s" %(elapsed))    # 时间长？？60ms
 
def search_hot_filter_test():
    
    """
    搜索热搜测试
    """
    start = time.time()    
    pathDir=os.path.dirname(__file__)
    path_index_hot_filter = os.path.join(pathDir, 'index_hot_all_filter','index_hot')
    area_code='-1'
    index_hot_filter=path_index_hot_filter+"_"+area_code
    sc_hot=open_dir(index_hot_filter).searcher()
    # results_hot_list=list(sc_hot.lexicon("goods_short"))##好像是默认字母顺序
    # results_hot_list=[str(b, encoding = "utf-8") for b in results_hot_list]  
    myquery = Every()
    results_hot=sc_hot.search(myquery, limit=None, sortedby='search_frequency', reverse=True)#limit=None返回所有结果
    results_hot_list=[i['goods_short'] for i in results_hot]
    # results_hot_list=[i['search_frequency'] for i in results_hot]
    print(results_hot_list)
    elapsed = time.time() - start
    print("Time used:%s s" %(elapsed))    # 时间长？？60ms


def wh_seg_test():
    schemaIndex1 = Schema(title=TEXT(stored=True, analyzer=NgramTokenizer(minsize=1, maxsize=1)))
    schemaIndex2 = Schema(title=TEXT(stored=True, analyzer=NgramTokenizer(minsize=1, maxsize=4)))
    schemaIndex3 = Schema(title=TEXT(stored=True, analyzer=ChineseAnalyzer()))
    schemaIndex3 = Schema(title=TEXT(stored=True, analyzer=ChineseAnalyzerMerge()))

    pathDir = os.path.dirname(__file__)



    path_index = os.path.join(pathDir, 'testsome', 'index_test')
    if not os.path.exists(path_index):
        os.makedirs(path_index)

    import whoosh.index
    a=whoosh.index.version_in(path_index)
    print(a)

    # >> > with myindex.writer() as w:

    # ix=create_in(path_index, schemaIndex3)
    ix=open_dir(path_index)
    writer = ix.writer()
    writer.add_document(title='崂山加强型自嗨锅啊 呵呵  巧克力绿茶味  增加了测试')
    # 如果多次增加索引？？ 直接修改源码？？？Chinese
    writer.commit(optimize=True)

    ix=open_dir(path_index)
    with ix.reader() as ir:
        segword = ir.lexicon('title')
        a=[i.decode() for i in segword]
    print(a)
    print(len(a), len(set(a)))
    # a = [1, 3, 5, 2]
    # b = [2, 4, 6]

    # for c in heapq.merge(a, b):
    #     print(c)

    # c=chain(a, b)
    # for i in c:
    #     print(type(c))
    #     print(i)

if __name__=='__main__':
    
    # search_asso_test()
    # results_hot=search_hot_whoosh_test()# whoosh查询8ms---
    
    # search_hot_mysql_test()# mysql查询200ms
    # search_hot_filter_test()
    # 分词测试
    wh_seg_test()


    """
     # 加一个过滤，实时读取在线简称。布隆过滤，过滤，只保留指定小区的上架商品
     # 可以这样做，建立一个索引和spu_code的组装信息，然后直接过滤spu_code
     # 或者，系数定时更新，或者实时更新，不用每次都过滤啊
     
     # 商品简称或者上下架或小区等数据有改动的时候，更新index
     
     # 分小区建立商品索引index_2018221,
     # 即在小区的上架商品中建立索引，一旦小区或者上架发生变化，
     # 需要重新建立索引，或者增加或者删除索引
     
     # java外包，上下架和小区商品数据更新变动的时候，需要读取数据库简称集合，
     # 比较现在的index反向索引集合，如果有增加或者减少，则更新index，否则不变。
     
     # 而搜索或搜索联想模块，与之相互独立，只需要检测index文件是否更新即可。
     # 4.编辑和删除索引：
    
     # 删除操作使用writer的以下三个方法：
     # delete_document(docnum)方法  (docnum是索引查询结果的每条结果记录的docnum属性)
     # delete_by_term(field_name,termtext)方法  （特别适合删除ID,或KEYWORD字段）
     # delete_by_query(query)
    
     # 添加操作使用writer的以下方法：
    """     
        


    