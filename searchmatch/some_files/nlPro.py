# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 10:10:51 2019

@author: lijiangman
"""

#nlPro

from imp import reload

import jieba
import re
import sys
# import os
# os.chdir('../../server')##change目录
# print(os.getcwd())
#################
#del jieba
#import jieba
#################

#应该加入“维达”，“软抽”这两个商品名称，商品品牌
  
#家庭清洁/纸品>清洁纸品>抽纸>维达（Vinda） >维达抽纸 
from jieba import analyse

#reload(analyse)##放到这里才能重新加载、、、

pattern = re.compile('[0-9]+')

#path="D:\\hisense\\speechR\\searchMatch\\stopwords-master\\"
#stw_all_path=path+'stop_words_all'


#reload(analyse)#重新加载analyse模块
##只运行一次，加载一次库
####reload(jieba)#加载过其他参数load_userdict，需要重新reload
##加入品牌和商品的词库字典，用于分词！
#    jieba.load_userdict("./utils/words_brand_goods.txt")
jieba.load_userdict("../searchMatchV2/utils/words_brand_goods_all.txt")
#    jieba.load_userdict("./utils/words_empty.txt")
#    jieba.load_userdict("./utils/食品日用品.scel.转换text.text")#词库数目会影响某词不存在词库的情况下，默认平均或中值idf
#    reload(jieba)#加载过其他参数load_userdict，需要重新reload
#    jieba设置自己的词典之后，不能返回重置原来的内置词典了。可以del jieba再import
#    放弃，还是关掉spyder重新打开吧

##这个词库不能使用，因为有“纯白毛巾”这个词！！分词不能把“纯白”和“毛巾”分开
##这个词库不能使用，因为有“纯白毛巾”这个词！！分词不能把“纯白”和“毛巾”分开
##这个词库不能使用，因为有“纯白毛巾”这个词！！分词不能把“纯白”和“毛巾”分开
##先load字典，再import analyse
####reload(analyse)#重新加载analyse模块
stw_all_path="../searchMatchV2/utils/stop_words_all"
analyse.set_stop_words(stw_all_path)#停用词过滤

# 创建停用词列表
def stopwordslist(stw_all_path):

    stopwords = [line.strip() for line in open(stw_all_path,encoding='UTF-8').readlines()]
    return stopwords
##增加了jieba的分词的停用词设置
stopwords = stopwordslist(stw_all_path)



##jieba的基本设置
#def jieba_set():
#    reload(jieba)#加载过其他参数load_userdict，需要重新reload
#    ##加入品牌和商品的词库字典，用于分词！
##    jieba.load_userdict("./utils/words_brand_goods.txt")
#    jieba.load_userdict("./utils/words_brand_goods_all.txt")
##    jieba.load_userdict("./utils/words_empty.txt")
##    jieba.load_userdict("./utils/食品日用品.scel.转换text.text")#词库数目会影响某词不存在词库的情况下，默认平均或中值idf
##    reload(jieba)#加载过其他参数load_userdict，需要重新reload
##    jieba设置自己的词典之后，不能返回重置原来的内置词典了。可以del jieba再import
##    放弃，还是关掉spyder重新打开吧
#    
#    ##这个词库不能使用，因为有“纯白毛巾”这个词！！分词不能把“纯白”和“毛巾”分开
#    ##这个词库不能使用，因为有“纯白毛巾”这个词！！分词不能把“纯白”和“毛巾”分开
#    ##这个词库不能使用，因为有“纯白毛巾”这个词！！分词不能把“纯白”和“毛巾”分开
#    ##先load字典，再import analyse
#    reload(analyse)#重新加载analyse模块
#    stw_all_path="./utils/stop_words_all"
#    analyse.set_stop_words(stw_all_path)#停用词过滤
#

##商品名称关键词提取
def key_word_good_extract(text):
    
    

    #key_word_tags = analyse.extract_tags(text,topK=None,withWeight=True)#通过tf-idf去除停用词
    #print(key_word_tags)
    ##用词性进行限制关键词，比如名词！！！
    
    #不同的句子里面，“洗发水”的idf值不同啊。因为用总的词数归一化了！
    # 源代码里面把小于1的也排除了！len(wc.strip()) < 2 or wc.lower()
    #万一有人搜索“花”呢。。修改
    ##
    #reload(jieba)

    #应该提取出“维达”，“软抽”这两个商品名称，商品品牌
    #我们的商品数据库分词名称，一定要加入搜索的分词数据库，这样，用户搜的商品
    #分词之后，一定可以分开。如果两个词都加入了词库呢？
    #比如苹果，苹果醋，
    #没有影响，都是分词为“苹果醋”
    #我自己定义了一个“软抽”和“软抽发”，结果都是软抽发。
    #难道像鹏鹏说的，自动匹配最长的字段？
    #
    #
    #要把spu_name的名字、品牌等单独作为第一大类，然后再进行商品描述
    #
    #多用途小麦粉
    #一级压榨油
    #
    #比如花生油--一级、二级
    #小麦粉--多用途
    #奇异果、猕猴桃、弥猴桃
    #mi猴桃，ni猴桃
    ##
    #text='我想要买一个高弹性的牛仔裤'  ##高弹性的袜子---用户搜索
    
    #text='心畅软抽纸巾'
    #reload(analyse)
#    seg_list=list(jieba.cut(text,cut_all=False,HMM=True))
#    seg_list=jieba.posseg.cut(text)
#    for i in seg_list:
#        print(i,':',i.flag)
    key_word_tags = jieba.analyse.extract_tags(text,topK=None
                                         ,withWeight=True
                                         ,allowPOS=(['nz','n']))#通过tf-idf去除停用词
    print(key_word_tags)
    key_num=len(key_word_tags)
    key_word_tags_inv=[]
    for i in key_word_tags:
        tag_tuple=(i[0],i[1]*key_num)
        key_word_tags_inv.append(tag_tuple)#反归一
    print(key_word_tags_inv)

    goods_set=set()
    for i in key_word_tags_inv:
        matchNum = pattern.findall(i[0])##包含数字
        if (i[1]>11 and not bool(re.search('[a-z]', i[0].lower())) and  not matchNum):
            print('good:',i[0])
#            if i[0]=='人食':
#                print(text)
#                sys.exit()
            goods_set.add(i[0])
    return goods_set
#    print(seg_list)
    #商品名字过滤
#    path="./食品日用品.scel.转换text.text"
#    good_words=set()
#    content = open(path, 'rb').read().decode('utf-8')
#    for line in content.splitlines():
#        good_words.add(line)
#    
#    seg_list_filter=[]
#    for i in seg_list:
#        if i.lower() in good_words:
#            seg_list_filter.append(i)
#            
#    print(seg_list_filter)
    
##品牌关键词提取
def key_word_brand_extract(text,brand_set):
    
    key_word_tags = jieba.analyse.extract_tags(text,topK=None
                                         ,withWeight=True
                                         ,allowPOS=(['nz','n']))#通过tf-idf去除停用词
    print(key_word_tags)
    key_num=len(key_word_tags)
    key_word_tags_inv=[]
    for i in key_word_tags:
        tag_tuple=(i[0],i[1]*key_num)
        key_word_tags_inv.append(tag_tuple)#反归一
    print(key_word_tags_inv)


    brand_extr_set=set()
    for i in key_word_tags_inv:
#        matchNum = pattern.findall(i[0])##包含数字
        matchBrand=(i[0] in brand_set)
        if (i[1]>6 and matchBrand):
                print('brand:',i[0])
#            if i[0]=='人食':
#                print(text)
#                sys.exit()
                brand_extr_set.add(i[0])
    return brand_extr_set

##用户请求的关键词提取
def key_word_extract(text):
    
    key_word_tags = analyse.extract_tags(text,topK=None
                                         ,withWeight=True)
#                                         ,allowPOS=(['nz','n']))#通过tf-idf去除停用词

    key_word_set=set()#精准分词
    for i in key_word_tags:
        key_word_set.add(i[0])


#    print('jingzhun:',key_word_tags)
#    key_num=len(key_word_tags)
#    key_word_tags_inv=[]
#    for i in key_word_tags:
#        tag_tuple=(i[0],i[1]*key_num)
#        key_word_tags_inv.append(tag_tuple)#反归一
#    print(key_word_tags_inv)
    words_cut_all=jieba.cut(text,cut_all=True,HMM=True)
    
    words_cut_all_set=set()
    for i in words_cut_all:
        if i not in stopwords:
            words_cut_all_set.add(i)
        
    ##为了防止“手撕”、“刀月”被分开，把用户原始输入加入集合
    if text not in stopwords:
        words_cut_all_set.add(text)
    
    key_word_set_all=key_word_set|words_cut_all_set
    
#    print('quan:',words_cut_all_set)
#    if '' in key_word_set_all:
#        key_word_set_all.remove('')#删除空--避免两个空格造成的结果
    
    while '' in key_word_set_all:
        key_word_set_all.remove('')


#    return (key_word_set,words_cut_all_set)
    return key_word_set_all

##精准分词
def key_word_extract_jingzhun(text):
    
    key_word_tags = analyse.extract_tags(text,topK=None
                                         ,withWeight=True)
#                                         ,allowPOS=(['nz','n']))#通过tf-idf去除停用词
    key_word_set=set()#精准分词
    for i in key_word_tags:
        key_word_set.add(i[0])
        
    key_word_list=list(key_word_set)
    return key_word_list

##分词库生成
def seg_words_set_generate(path,goods_words):
#    path_list=['百度停用词表.txt','哈工大停用词表.txt','四川大学机器智能实验室停用词库.txt','中文停用词表.txt']
#    for i in path_list:
    with open(path,'r',encoding='UTF-8') as lines:
        goods_words_list=lines.readlines()
    
    ##删除换行符
    goods_words_set=set()
    for i in goods_words_list:
        goods_words_set.add(i.replace('\n',''))
    ##删除set中空元素 删除空
    if '' in goods_words_set:
        print('yesss')
        goods_words_set.remove('')#删除空--防止文本最后一行是空
    
    
    goods_words_set_all=goods_words.union(goods_words_set)
#
#    path_write=path+'stop_words_all'
#    with open(path_write,'w',encoding='UTF-8') as f:
#        for i in stw_set:
#            f.write(i)
    return goods_words_set_all

##测试
if __name__=='__main__':
    # print(os.getcwd())
    # os.chdir('../../server')##change目录
    # print(os.getcwd())
    text='烟酒'
    key_word_set_all=key_word_extract(text)
    print(key_word_set_all)
#    goods_set=set()
#    for i in key_word_tags_inv:
#        matchNum = pattern.findall(i[0])
#        if (i[1]>11 and not bool(re.search('[a-z]', i[0].lower())) and  not matchNum):
#            print('good:',i[0])
##            if i[0]=='人食':
##                print(text)
##                sys.exit()
#            goods_set.add(i[0])
#    return goods_set
#    print(seg_list)
    #商品名字过滤
#    path="./食品日用品.scel.转换text.text"
#    good_words=set()
#    content = open(path, 'rb').read().decode('utf-8')
#    for line in content.splitlines():
#        good_words.add(line)
#    
#    seg_list_filter=[]
#    for i in seg_list:
#        if i.lower() in good_words:
#            seg_list_filter.append(i)
#            
#    print(seg_list_filter)
