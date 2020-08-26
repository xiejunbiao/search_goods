# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 17:50:11 2019

@author: lijiangman
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 17:52:16 2019

@author: lijiangman
"""

##商品名称提取，然后需要手动编辑修改
##把数据库中的分词都加入jieba分词库

from imp import reload

#from utils import dataBasePro,nlPro
#import dataBasePro,nlPro
from utils import dataBasePro,nlPro 

#from dataBasePro import readMysqlPd,segSpu,stop_words_set_generate,soug_generate
from utils.dataBasePro import readMysqlPd,segSpu,soug_generate,write2File,write2File2,read2Var
from utils.dataBasePro import stop_words_set_generate,seg_words_set_generate
#python多次重复使用import语句时，不会重新加载被指定的模块，
#只是把对该模块的内存地址给引用到本地变量环境。
#reload(nlPro)
#reload('D:\\hisense\\speechR\\searchMatch\\utils\\nlPro')
#reload(nlPro)#为了reload结巴分词，这里和nlPro里面都要reload
#from nlPro import key_word_set

#from utils.nlPro import key_word_good_extract
from querySemExterAnalyse import spu_name_extra,spu_brand_extra


#import pandas as pd
#from pandas import Series,DataFrame
import matplotlib.pyplot as plt 


#reload(jieba)

plt.close('all')
##读取数据库模块
ip="10.18.222.114"
userName="root"
password="123456"
db="hisense"
dbTable="goods_spu"#订单
#dbTable="goods_spu_test2"#复制了一份

spu_name='spu_name'
filterSql=""#过滤sql语句
dataDf=readMysqlPd(ip,userName,password,db,dbTable,filterSql)
spu_names=dataDf[spu_name]

##
##通过tf-idf去除停用词
#dian_shang_by_me=set(['想要\n','想\n','一瓶\n','买\n','一支\n','片\n'
#                      ,'支装\n','寸\n'
#                      ,'小号\n','款\n'
#                      ,'十件\n','个瓜\n'
#                      ,'件套\n'])#我自己编辑的电商常用语
#path="D:\\hisense\\speechR\\searchMatch\\stopwords-master\\"
#stw_all_path=path+'stop_words_all'
#stw_set=stop_words_set_generate(path,dian_shang_by_me)#停用词集合
##nlp自然语言处理阶段
#text='维达超韧软抽'#['维达', '超韧软', '抽']分词错误----商品数据库
#text='我想要买苹果醋'
text='我想要买软抽发' 
 
text='我想要买纯白毛巾' 
#text='龙安消毒液'
#text='可爱消毒液'
#text='我想要买海信高清电视'
text='三只松鼠黄桃干1袋106g'

#个瓜 人食 是不是因为有空格！
#新疆天润蜜了个瓜酸奶 180g*12袋/箱

##商品名称提取
path='./utils/goods_spu'
spu_name_extra(spu_names,path)

path='./utils/brand_spu'
path_brand='./utils/words_brand.txt'#品牌路径
brand_set=read2Var(path_brand)
spu_brand_extra(spu_names,path,brand_set)
#goods_set=set()
#goods_list=[]
#for i in spu_names:
#    goods=key_word_good_extract(i)
#    goods_list.append([i,goods])
#    goods_set=goods_set|goods
#
#print(goods_set,len(goods_set))
#path='./utils/goods_spu'
###为啥每次set的顺序不一样啊
##print打印之后元素顺序是随机的。这是因为集合(set)是Python中一种重要的数据类型，
##表示一组各不相同元素的无序集合，其主要应用于重复元素消除及关系测试等 
#write2File(path,goods_list)


###商品集合
#dbTable="goods_spu_test9"#
#filterSql=""#过滤sql语句
#dataDf=readMysqlPd(ip,userName,password,db,dbTable,filterSql)
#spu_names=dataDf['spu_g_name']
#
###把数据库中的分词都加入jieba分词库
#goods_words=set()#商品分词集合
#for i in spu_names:
#    goods=i.split(" ")
#    for j in goods:
#        goods_words.add(j)
#
#if '' in goods_words:
#    goods_words.remove('')#删除空
#path='./utils/words_brand_goods_ours.txt'#我们平台的品牌和商品词库
#write2File2(path,goods_words)
#
###综合两个品牌和商品库
#path='./utils/words_brand_goods.txt'#网上的品牌和商品词库
##path='./words_brand_goods.txt'#网上的品牌和商品词库
#goods_words_set_all=seg_words_set_generate(path,goods_words)
#path='./utils/words_brand_goods_all.txt'#综合的品牌和商品词库
#write2File2(path,goods_words_set_all)

##语义扩展模块

#同一个商品spu，可能有多个title或name，比如维达、软抽对应同一个spu
#name里面有两个分词，“海飞丝”、“洗发水”
#两步：
#1、商品--必须把商品分成多个词，“海飞丝”、“洗发水”，
#否则就会出现“一级花生油”在spu，用户的“一级花生油”，就有问题
#
#如果法丽兹 蛋卷
#法丽兹 饼干 
#海信 电视
#海信 洗衣机
#############
#顺序是商品，品牌，描述
#电视->海信->高清
#否则就会出现，搜索海信冰箱，但是匹配了海信电视
#鹏鹏
#######
#2、具体描述 ：高弹性
#龙安消毒液，应该提取出“龙安”，“消毒液”

#spu_names_seg=segSpu(spu_names,analyse)#spu名字分词
#dataDf['spu_name_seg']=spu_names_seg
#queryMostSimilar()

#path="D:\\hisense\\speechR\\searchMatch\\食品日用品.scel.转换text.text"
#我增加了
#心畅软抽纸巾
#心畅
#软抽
#纸巾
















