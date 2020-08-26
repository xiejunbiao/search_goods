# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 17:36:28 2019

@author: lijiangman
"""

##重新生成商品信息，写入mysql数据库
##0、类别1、商品2、品牌3、特征



from imp import reload
import pymysql

#from querySemExterAnalyse import jieba_word_path_gener,create_spu_table
from dataBasePro import jieba_word_path_gener,create_spu_table
#from utils import dataBasePro,nlPro
#import dataBasePro,nlPro
#from utils import dataBasePro,nlPro 

#from dataBasePro import readMysqlPd,segSpu,stop_words_set_generate,soug_generate
#from utils.dataBasePro import readMysqlPd,segSpu,stop_words_set_generate,soug_generate
#from utils.dataBasePro import seg_words_set_generate,write2File2,write2File,read2Var,read2Var2

from dataBasePro import readMysqlPd,segSpu,stop_words_set_generate,soug_generate
from dataBasePro import seg_words_set_generate,write2File2,write2File,read2Var,read2Var2

#python多次重复使用import语句时，不会重新加载被指定的模块，
#只是把对该模块的内存地址给引用到本地变量环境。
#reload(nlPro)
#reload('D:\\hisense\\speechR\\searchMatch\\utils\\nlPro')
#reload(nlPro)#为了reload结巴分词，这里和nlPro里面都要reload
#from nlPro import key_word_set

#from utils.nlPro import key_word_good_extract


#import pandas as pd
#from pandas import Series,DataFrame
import matplotlib.pyplot as plt 

##新建一个spu商品表

#reload(jieba)

plt.close('all')
##读取数据库模块
ip="10.18.222.114"
userName="root"
password="123456"
#db="hisense"
db="hisense_ljm"
#dbTable="goods_spu"#
dbTable="goods_spu_edit4"#

spu_name='spu_name'
port=3306
filterSql=""#过滤sql语句
dataDf=readMysqlPd(ip,userName,password,db,dbTable,filterSql)
spu_names=dataDf[spu_name]
spu_codes=dataDf['spu_code']
##增加一列
##0、类别1、商品2、品牌3、特征
##新建spu商品名称表
#good_spu_edit_file='goods_spu_edit4'
good_spu_edit_file='goods_spu_edit5'##修改屠龙刀、123
#good_spu_edit_path='./utils/'+good_spu_edit_file
good_spu_edit_path='./'+good_spu_edit_file
good_spu_edit=read2Var2(good_spu_edit_path)
connect=pymysql.connect(host=ip,user=userName, password=password,port=port)
cursor = connect.cursor()
sqlDB='''USE '''+db
cursor.execute(sqlDB)
#由于刚开始的时候 TESTDB 数据库中没有 EMPLOYEE 表，
#所以程序在执行cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")语句的时候报了如下提醒信息


##新建一个spu商品表
dbTable=good_spu_edit_file
create_spu_table(dbTable,connect,cursor,spu_names,spu_codes,good_spu_edit)
##
##商品集合--jieba分词库路径生成
filterSql=""#过滤sql语句
dataDf=readMysqlPd(ip,userName,password,db,dbTable,filterSql)
spu_names=dataDf['spu_g_name']

jieba_word_path_gener(spu_names)








