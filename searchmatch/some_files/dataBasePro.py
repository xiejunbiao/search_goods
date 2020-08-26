# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 17:19:20 2019

@author: lijiangman
"""
#数据库读取类
# python2  import MySQLdb
import pymysql # python3  
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
import copy

from searchMatchV2.utils.nlPro  import key_word_extract_jingzhun
#import time
#import json

#import datetime##最好不用后面的这个from datetime import datetime

#import jieba
#from wpp.Predata import date2whatday
#from wpp.Predata import date2week


#####################################################
##输出pandas结构
def readMysqlPd(ip,userName,password,db,dbTable,filterSql):
    # 打开数据库连接
    db = pymysql.connect(ip,userName,password,db)
#    db = pymysql.connect("10.18.226.56","root","123456","hisense" )
#    print(db)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 使用 execute()  方法执行 SQL 查询 
#    cursor.execute("SELECT VERSION()")
#    mysqlCmd="SELECT interface FROM adapter_record" 
#    mysqlCmd="SELECT * FROM adapter_record" 
    mysqlCmd="SELECT * FROM "+ dbTable + filterSql
    # print(mysqlCmd)
#    SELECT DISTINCT LEFT(order_no,8) FROM hjmall_order  WHERE is_pay=1
    ##
    dataPd=pd.read_sql(mysqlCmd,db)
    ##字段描述
    mysqlCmd="desc "+dbTable
    cursor.execute(mysqlCmd)
    # 使用 fetchone() 方法获取单条数据.  data = cursor.fetchone()
    # 使用 fetchall() 方法获取所有数据.
#    print("describe:\n")
#    data = cursor.fetchall()
#    for row in data:
#        interf=row[0]
#        print("describe=", row)
    ##
#    统计多少行。。。。
    cursor.execute("select count(*) from "+dbTable)
#    data=cursor.fetchall()
#    print(type(data))##tuple
#    print("rows: ",data[0][0])
    ##
    mysqlCmd="SELECT * FROM "+ dbTable + " limit 2"
    cursor.execute(mysqlCmd)
#    data = cursor.fetchall()
#    print("select:\n")
#    for row in data:
#        print("select=", row)
    # 关闭数据库连接
    db.close()
    return dataPd

def readMysqlPd_for_search(ip,userName,password,db,mysqlCmd):
    # 打开数据库连接
    db = pymysql.connect(ip,userName,password,db)
#    db = pymysql.connect("10.18.226.56","root","123456","hisense" )
#    print(db)
    # 使用 cursor() 方法创建一个游标对象 cursor
    # cursor = db.cursor()
    # 使用 execute()  方法执行 SQL 查询 
#    cursor.execute("SELECT VERSION()")
#    mysqlCmd="SELECT interface FROM adapter_record" 
#    mysqlCmd="SELECT * FROM adapter_record" 
    # mysqlCmd="SELECT * FROM "+ dbTable + filterSql
    # print(mysqlCmd)
#    SELECT DISTINCT LEFT(order_no,8) FROM hjmall_order  WHERE is_pay=1
    ##
    dataPd=pd.read_sql(mysqlCmd,db)
    ##字段描述
    # mysqlCmd="desc "+dbTable
    # cursor.execute(mysqlCmd)
    # 使用 fetchone() 方法获取单条数据.  data = cursor.fetchone()
    # 使用 fetchall() 方法获取所有数据.
#    print("describe:\n")
#    data = cursor.fetchall()
#    for row in data:
#        interf=row[0]
#        print("describe=", row)
    ##
#    统计多少行。。。。
    # cursor.execute("select count(*) from "+dbTable)
#    data=cursor.fetchall()
#    print(type(data))##tuple
#    print("rows: ",data[0][0])
    ##
    # mysqlCmd="SELECT * FROM "+ dbTable + " limit 2"
    # cursor.execute(mysqlCmd)
#    data = cursor.fetchall()
#    print("select:\n")
#    for row in data:
#        print("select=", row)
    # 关闭数据库连接
    db.close()
    return dataPd


def segSpu(spu_names,analyse):
    spu_names_seg=[]
    for i in spu_names:
        key_word_tags = analyse.extract_tags(i,topK=None,withWeight=True)
        #seg_list=list(jieba.cut(i,cut_all=False))
        spu_names_seg.append(key_word_tags)

    return spu_names_seg





def stop_words_set_generate(path,dian_shang_by_me):
#    dian_shang_by_me=set(['想要\n','一瓶\n'])#我自己编辑的电商常用语
    stw_set_list=[]
    path_list=['百度停用词表.txt','哈工大停用词表.txt','四川大学机器智能实验室停用词库.txt','中文停用词表.txt']
    for i in path_list:
        path_stw=path+i
        with open(path_stw,'r',encoding='UTF-8') as lines:
            stw_list=lines.readlines()
        stw_set_list.append(set(stw_list))
    stw_set={}
    for i in stw_set_list:
        ##无法删除换行符
#        ir=i.replace('\n','')
        stw_set=i.union(stw_set)
        
    stw_set=dian_shang_by_me.union(stw_set)

#    path_write=path+'stop_words_all'
#    with open(path_write,'w',encoding='UTF-8') as f:
#        for i in stw_set:
#            f.write(i)
    return stw_set

###分词库生成
#def seg_words_set_generate(path,goods_words):
##    path_list=['百度停用词表.txt','哈工大停用词表.txt','四川大学机器智能实验室停用词库.txt','中文停用词表.txt']
##    for i in path_list:
#    with open(path,'r',encoding='UTF-8') as lines:
#        goods_words_list=lines.readlines()
#    
#    ##删除换行符
#    goods_words_set=set()
#    for i in goods_words_list:
#        goods_words_set.add(i.replace('\n',''))
#    ##删除set中空元素 删除空
#    if '' in goods_words_set:
#        print('yesss')
#        goods_words_set.remove('')#删除空--防止文本最后一行是空
#    
#    
#    goods_words_set_all=goods_words.union(goods_words_set)
##
##    path_write=path+'stop_words_all'
##    with open(path_write,'w',encoding='UTF-8') as f:
##        for i in stw_set:
##            f.write(i)
#    return goods_words_set_all
        

def soug_generate(path):
    with open(path,'r',encoding='UTF-8') as lines:
        stw_list=lines.readlines()
    
    return stw_list

def write2File(path,f_set):
    with open(path,'w',encoding='UTF-8') as f:
#        for i in f_set:
#            f.write(i+'\n')
        for i in f_set:
            f.write(i[0]+'$$$$$')
            for j in i[1]:
                f.write(j+' ')
            f.write('\n')

def write2File2(path,f_set):
    set_len=len(f_set)
    with open(path,'w',encoding='UTF-8') as f:
        iterNum=0##最后一行不用回车
        for i in f_set:
            iterNum=iterNum+1
            ##删除换行符
            ir=i.replace('\n','')
            if iterNum<set_len:
                f.write(ir+'\n')
            else:
                f.write(ir)

def read2Var(path):
    with open(path,'r',encoding='UTF-8') as lines:
        text_list=lines.readlines()
    
    text_set=set()
    for i in text_list:
        ir=i.replace('\n','')
        text_set.add(ir)
    
    return text_set




def read2Var2(path):
    with open(path,'r',encoding='UTF-8') as lines:
        text_list=lines.readlines()
    
    text_list_re=[]
    for i in text_list:
        ir=i.replace('\n','')
        ir=ir.strip()#去除首尾空格
        text_list_re.append(ir)
    
    return text_list_re




##新建一个spu简称集合表
def create_spu_short_table(dbTable,dbTable1):
    ip="10.18.226.38"##集成测试数据库
#    ip="10.18.222.114"##防止清库！在自己的数据库中进行人工编辑
#    db="goods_spu_search"##防止清库！在自己的数据库中进行人工编辑
    db="hisense"##防止清库！在自己的数据库中进行人工编辑
    userName="root"
    password="123456"
#    filterSql=' WHERE goods_short !="" GROUP BY goods_short'
    # filterSql=' WHERE goods_short !="" GROUP BY goods_short_edit'
    ##读取商品简称和品牌
    dbc = pymysql.connect(ip,userName,password,db)

    mysqlCmd="SELECT gss.goods_short FROM goods_spu_search gss WHERE goods_short !='' GROUP BY goods_short" 
    dataDf=pd.read_sql(mysqlCmd, dbc)
    goods_short=dataDf['goods_short']
    goods_short=set(goods_short)##商品简称集合

    mysqlCmd="SELECT gss.goods_brand FROM goods_spu_search gss WHERE goods_brand !=''  AND goods_brand !='--' GROUP BY goods_brand"
    dataDf=pd.read_sql(mysqlCmd, dbc)
    goods_brand=dataDf['goods_brand']
    goods_brand=set(goods_brand)##商品简称集合


    # print(dataDf.columns)
    # 关闭数据库连接
    dbc.close()
    # dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
    
    goods_short_set = goods_short | goods_brand
    
    ##新建表
#    dbTable='goods_spu_short_set'
    port=3306
    connect=pymysql.connect(host=ip,user=userName, password=password,port=port)
    cursor = connect.cursor()
    sqlDB='''USE '''+db
    cursor.execute(sqlDB)
    sql5='''drop table  if exists '''+dbTable
    cursor.execute(sql5)
    sql='''create table '''+dbTable+''' ( goods_short char(100))'''
    
    #id自增
#    sql='''create table '''+dbTable+''' (good_id int PRIMARY key auto_increment, spu_code char(100), spu_name char(100), spu_g_name char(100))'''
    cursor.execute(sql)
#    for i in range(spu_len):
#        good_short=goods_short[i]
#        spu_code=spu_codes[i]
    for goods_short in goods_short_set:
        sql1 = 'INSERT INTO ' +dbTable+' (goods_short) VALUES ('+  "'"+  goods_short + "'"+  ')'
        cursor.execute(sql1)
    connect.commit()
    connect.close()
    
    return goods_short_set
  
def get_goods_short_seg(goods_short_set):
    
    goods_short_set_seg=set()
    for i in goods_short_set:
        i_tep=i.replace(',',' ')##有的是逗号分隔
        i_tep=i_tep.replace('\n','')##回车
        j_spl=i_tep.split(' ')
#        if '' in j_spl:##这个语句只能删除一次空！！！
        while '' in j_spl:
            j_spl.remove('')
        for j in j_spl:
            goods_short_set_seg.add(j)
        
    
    return goods_short_set_seg
#首先加入分词库，保证分词成功。然后商品库集合自动化标注
#或者先不加，直接计算字符串包含简称集合的元素个数，自动商品简称标注
#---这样可能会出现，“肉色”包含“肉”，不准确！
#应该分词匹配！
#    日本设计 除菌除氯净水龙头净水器套装（1机身+1滤芯）  被错划分到“水 纯净水”
def get_spu_short(spu_name,goods_short_set):
    spu_name_list=key_word_extract_jingzhun(spu_name)
    gs_dict={}
    for i in goods_short_set:
        cnt_all=0
        j_spl=i.split(' ')
        if '' in j_spl:
            j_spl.remove('')
        for j in j_spl:
#            cnt_all=cnt_all+spu_name_list.count(j)
            cnt_all=cnt_all+spu_name_list.count(j)*len(j)##“水” 错分 “资生堂水之密语净澄水活护发素1瓶600ml”--解决方法：增加字数权重
        gs_dict[i]=cnt_all
    
#    a1=sorted(gs_dict,reverse=False)###不行！
#    gs_dict_deepcopy=copy.deepcopy(gs_dict)
    a1 = sorted(gs_dict.items(), key = lambda x:x[1],reverse=True)
    good_short=a1[0][0]
    gs_cnt=a1[0][1]
    if gs_cnt==0:##没有此商品
       good_short='no'
    return good_short


##新建一个spu简称集合表--检查自动化处理商品简称是否正确
def create_spu_short_table_check(dbTable,goods_short_set):
#    ip="10.18.226.33"##集成测试数据库
    ip="10.18.222.114"##防止清库！在自己的数据库中进行人工编辑

    dbTable1="goods_spu_search"##最新的搜索数据库
#    db="hisense"
    db="goods_spu_search"##防止清库！在自己的数据库中进行人工编辑
    userName="root"
    password="123456"
    filterSql=' WHERE goods_short !="" GROUP BY goods_short'
    dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
    spu_codes=dataDf['spu_code']
    goods_short=dataDf['goods_short']
    spu_names=dataDf['spu_name']
    ##新建表
#    dbTable='goods_spu_short_set'
    port=3306
    connect=pymysql.connect(host=ip,user=userName, password=password,port=port)
    cursor = connect.cursor()
    sqlDB='''USE '''+db
    cursor.execute(sqlDB)
    sql5='''drop table  if exists '''+dbTable
    cursor.execute(sql5)
    sql='''create table '''+dbTable+''' (spu_code char(100), goods_short char(100), spu_name char(100))'''
    
    #id自增
#    sql='''create table '''+dbTable+''' (good_id int PRIMARY key auto_increment, spu_code char(100), spu_name char(100), spu_g_name char(100))'''
    cursor.execute(sql)
    spu_len=len(goods_short)
    for i in range(spu_len):
        spu_code=spu_codes[i]
        spu_name=spu_names[i]
#        good_short=''#置为空，以便检查check
        good_short=get_spu_short(spu_name,goods_short_set)

        sql1 = 'INSERT INTO ' +dbTable+' (spu_code,goods_short,spu_name) VALUES ('+  "'"+spu_code+"',"+ "'"+  good_short + "'"+",'"+ spu_name+"'"+ ')'
        cursor.execute(sql1)
    connect.commit()
    connect.close()
  


##新建一个spu商品表--加入商品名称
def create_spu_table(dbTable,connect,cursor,spu_names,spu_codes,good_spu_edit):
    sql5='''drop table  if exists '''+dbTable
    cursor.execute(sql5)
    #sql='''create table '''+dbTable+''' (spu_code char(100), spu_name char(100), spu_g_name char(100))'''
    
    #id自增
    sql='''create table '''+dbTable+''' (good_id int PRIMARY key auto_increment, spu_code char(100), spu_name char(100), spu_g_name char(100))'''
    cursor.execute(sql)
    
    spu_len=len(good_spu_edit)
    for i in range(spu_len):
        spu_name=spu_names[i]
        spu_code=spu_codes[i]
        i2=good_spu_edit[i].split('$$$$$')
        i3=i2[1]
        i4=i3.replace('\n','')
    #    i5=i4.rstrip()#去除尾空格
        spu_g_name=i4.strip()#去除首尾空格
        
        sql1 = 'INSERT INTO ' +dbTable+' (spu_code,spu_name,spu_g_name) VALUES ('+  "'"+spu_code+"',"+ "'"+  spu_name+"',"+ "'"+spu_g_name +"'"+')'
        cursor.execute(sql1)
    connect.commit()
    connect.close()
    
    

##jieba分词库路径生成
#def jieba_word_path_gener(spu_names):
#    ##把数据库中的分词都加入jieba分词库
#    goods_words=set()#商品分词集合
#    for i in spu_names:
#        goods=i.split(" ")
#        for j in goods:
#            goods_words.add(j)
#    
#    if '' in goods_words:
#        goods_words.remove('')#删除空
##    path='./utils/words_brand_goods_ours.txt'#我们平台的品牌和商品词库
#    path='./words_brand_goods_ours.txt'#我们平台的品牌和商品词库
#    write2File2(path,goods_words)
#    
#    ##综合两个品牌和商品库
#    path='./words_brand_goods.txt'#网上的品牌和商品词库
#    #path='./words_brand_goods.txt'#网上的品牌和商品词库
#    goods_words_set_all=seg_words_set_generate(path,goods_words)
#    path='./words_brand_goods_all.txt'#综合的品牌和商品词库
#    write2File2(path,goods_words_set_all)


##
# 只选出需要的字段
# 根据衣军成的建议list=",".join(list)
# mysql=  (  %s   )   % list 
def readGoodsSearch(ip,gss_tb,gs_tb,area_code,shop_code,db,words_cut_all,userName,password,sortMethod):

    ##mysql正则包含关键词
    words_cut_all=list(words_cut_all)

    wl=len(words_cut_all)
    if wl==0:
        df = pd.DataFrame()
        return df##空的dataFrame
    else:
        keyword_reg="|".join(words_cut_all) #    join就行
    
    # if wl==1:
    #     keyword_reg=words_cut_all[0]

    # if wl>1:
    #     keyword_reg=words_cut_all[0]
    #     for i in range(1,wl):
    #         keyword_reg=keyword_reg+"|"+words_cut_all[i]
        
    #############
    ##
    #就是子查询改成连接方式，不过我试了下现在挺快的呀，数据量大了效果能更明显
    # SELECT * FROM goods_spu_search a 
    # JOIN goods_scope b ON a.spu_code=b.spu_code 
    # AND b.area_code='2018012300005' 
    # AND  a.goods_status=1;
    #，area_code后面的值要加单引号

    ##mysql可以只返回需要的字段
    # area_code='2018012300005'
    # gss_tb='goods_spu_search'
    # gs_tb="goods_scope"
    # keyword_reg="草莓"
    # shopCode='356396279627776000'#海信广场超市
        
    a_col="a.spu_code, a.spu_name, a.goods_short\
        ,a.goods_brand\
        , a.spu_cate_third, a.spu_cate_third_edit\
        ,a.sale_price, a.sale_month_count, a.shop_name"        
    # if sortMethod=='null':##matchScore
        
    #     a_col="a.spu_code, a.spu_name, a.goods_short\
    #     ,a.goods_brand, a.spu_sale_point, a.spu_cate_first\
    #     ,a.spu_cate_second, a.spu_cate_third, a.spu_cate_third_edit\
    #     ,a.shop_name"
    # else:
    #     a_col="a.spu_code, a.spu_name, a.goods_short\
    #     ,a.goods_brand, a.spu_sale_point, a.spu_cate_first\
    #     ,a.spu_cate_second, a.spu_cate_third, a.spu_cate_third_edit\
    #     ,a.sale_price, a.sale_month_count, a.shop_name"
    
    # if shop_code=='-1':##没有shopcode

    #     mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
    #             and b.area_code='%s' \
    #             and a.goods_status=1 \
    #             and (a.spu_info_all REGEXP '%s' \
    #             or a.spu_info_all_edit REGEXP '%s' \
    #             or a.spu_name REGEXP '%s')" % (a_col, gss_tb, gs_tb, area_code, keyword_reg, keyword_reg, keyword_reg)
    # else:
        
    #     mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
    #             and b.area_code='%s' \
    #             and a.goods_status=1 \
    #             and a.shop_code='%s' \
    #             and (a.spu_info_all REGEXP '%s' \
    #             or a.spu_info_all_edit REGEXP '%s' \
    #             or a.spu_name REGEXP '%s')" % (a_col, gss_tb, gs_tb, area_code, shop_code, keyword_reg, keyword_reg, keyword_reg)
        
    if shop_code=='-1':##没有shopcode

        mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                and b.area_code='%s' \
                and a.goods_status=1 \
                and (a.spu_name REGEXP '%s' \
                or a.goods_short REGEXP '%s' \
                or a.goods_brand REGEXP '%s' \
                or a.spu_cate_third REGEXP '%s' \
                or a.spu_cate_third_edit REGEXP '%s' \
                or a.shop_name REGEXP '%s')" % (a_col, gss_tb, gs_tb, area_code, keyword_reg, keyword_reg, keyword_reg, keyword_reg, keyword_reg, keyword_reg)
    else:
        
        mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                and b.area_code='%s' \
                and a.goods_status=1 \
                and a.shop_code='%s' \
                and (a.spu_name REGEXP '%s' \
                or a.goods_short REGEXP '%s' \
                or a.goods_brand REGEXP '%s' \
                or a.spu_cate_third REGEXP '%s' \
                or a.spu_cate_third_edit REGEXP '%s' \
                or a.shop_name REGEXP '%s')" % (a_col, gss_tb, gs_tb, area_code, shop_code, keyword_reg, keyword_reg, keyword_reg, keyword_reg, keyword_reg, keyword_reg)
  
    ##join之后，返回结果多了两个字段：spu_code 和area_code，导致与之前的spu_code 重复
    # mysqlCmd="SELECT * FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
    #         and b.area_code='%s' \
    #         and a.goods_status=1 \
    #         and a.shop_code='%s' \
    #         and (a.spu_info_all REGEXP '%s' \
    #         or a.spu_info_all_edit REGEXP '%s' \
    #         or a.spu_name REGEXP '%s')" % (gss_tb, gs_tb, area_code, shop_code, keyword_reg, keyword_reg, keyword_reg)
        
    ##查询mysql数据库
    # print(mysqlCmd)
    dbc = pymysql.connect(ip,userName,password,db)
    ##
    dataDf=pd.read_sql(mysqlCmd, dbc)
    # print(dataDf.columns)
    # 关闭数据库连接
    dbc.close()

    return dataDf


##商铺过滤，用mysql    
def readGoodsSearchOld(ip,dbTable1,dbTable2,areaCode,shopCode,db,words_cut_all,userName,password):
    
##mysql正则包含关键词
    words_cut_all=list(words_cut_all)

    wl=len(words_cut_all)
    if wl==0:
        df = pd.DataFrame()
        return df##空的dataFrame
    
    if wl==1:
        keyword_reg=words_cut_all[0]

    if wl>1:
        keyword_reg=words_cut_all[0]
        for i in range(1,wl):
            keyword_reg=keyword_reg+"|"+words_cut_all[i]
#    SELECT * FROM goods_spu WHERE spu_name REGEXP  '苹果|葡萄|莱西'  AND goods_status=01
#    if shopCode=='8888ljm':##没有shopcode
###################
#    spu_info_all应该替换为spu_info_all_edit
    filterSql=""
    if shopCode=='-1':##没有shopcode
##        filterSql=" where goods_status=01"#过滤sql语句---#在线商品01--周生伟改为了1
#        filterSql=" where goods_status=1 "+ " and spu_name REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---#在线商品01--周生伟改为了1
#        filterSql=" where goods_status=1 "+ " and spu_info_all REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---#在线商品01--周生伟改为了1
#        SELECT * FROM goods_spu_search WHERE goods_status=1  AND (spu_info_all REGEXP  '酸奶' OR goods_short REGEXP '酸奶')
#        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"+" or "+"goods_short REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
#        filterSql=" where goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
        ##加入spu_info_all
#        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
        ##为了能够搜到网易严选的未清洗数据，加入spu_name
#        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'" +" )"#过滤sql语句---#在线商品1

    else:
##        filterSql=" where shop_code="+shopCode+" and goods_status=01"#过滤sql语句---商铺代码#在线商品    
#        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and spu_name REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---商铺代码#在线商品    
#        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and spu_info_all REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---商铺代码#在线商品    

#        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"+" or "+"goods_short REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
#        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
#        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
        ##加入spu_info_all
#        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'" +" )"#过滤sql语句---#在线商品1

        ###################    
#    dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
#    spu_codes=dataDf['spu_code']
    ##小区代码
#    dbTable2="goods_scope"##小区代码和spu代码对应关系
    if areaCode=='-1':
        filterSql=filterSql+" and spu_code in (select spu_code from goods_scope)"
    else:
        filterSql=filterSql+" and spu_code in (select spu_code from goods_scope where AREA_CODE=" + areaCode + ")"
    dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
#    dataDf_scope=readMysqlPd(ip,userName,password,db,dbTable2,filterSql)
    
#    spu_codes_area=dataDf_scope['spu_code']
#    spu_codes_area_set=set(spu_codes_area.tolist())#或者spu_codes_area.values
#    dataDf=dataDf[spu_codes.isin(spu_codes_area_set)]
    return dataDf


##商铺过滤，不用mysql    
#def readGoodsSearch(ip,dbTable1,dbTable2,areaCode,shopCode,db,words_cut_all,userName,password):
#    
###mysql正则包含关键词
#    words_cut_all=list(words_cut_all)
#
#    wl=len(words_cut_all)
#    if wl==0:
#        df = pd.DataFrame()
#        return df##空的dataFrame
#    
#    if wl==1:
#        keyword_reg=words_cut_all[0]
#
#    if wl>1:
#        keyword_reg=words_cut_all[0]
#        for i in range(1,wl):
#            keyword_reg=keyword_reg+"|"+words_cut_all[i]
##    SELECT * FROM goods_spu WHERE spu_name REGEXP  '苹果|葡萄|莱西'  AND goods_status=01
##    if shopCode=='8888ljm':##没有shopcode
####################
##    spu_info_all应该替换为spu_info_all_edit
#    filterSql=""
#    if shopCode=='-1':##没有shopcode
###        filterSql=" where goods_status=01"#过滤sql语句---#在线商品01--周生伟改为了1
##        filterSql=" where goods_status=1 "+ " and spu_name REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---#在线商品01--周生伟改为了1
##        filterSql=" where goods_status=1 "+ " and spu_info_all REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---#在线商品01--周生伟改为了1
##        SELECT * FROM goods_spu_search WHERE goods_status=1  AND (spu_info_all REGEXP  '酸奶' OR goods_short REGEXP '酸奶')
##        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"+" or "+"goods_short REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
##        filterSql=" where goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
#        ##加入spu_info_all
##        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
#        ##为了能够搜到网易严选的未清洗数据，加入spu_name
#        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
#
#    else:
###        filterSql=" where shop_code="+shopCode+" and goods_status=01"#过滤sql语句---商铺代码#在线商品    
##        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and spu_name REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---商铺代码#在线商品    
##        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and spu_info_all REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---商铺代码#在线商品    
#
##        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"+" or "+"goods_short REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
##        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
##        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
#        ##加入spu_info_all
#        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
#
#        ###################    
#    dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
#    spu_codes=dataDf['spu_code']
#    ##小区代码
##    dbTable2="goods_scope"##小区代码和spu代码对应关系
#    if areaCode=='-1':
#        filterSql=""
#    else:
#        filterSql=" where area_code="+areaCode#过滤sql语句
#    dataDf_scope=readMysqlPd(ip,userName,password,db,dbTable2,filterSql)
#    
#    spu_codes_area=dataDf_scope['spu_code']
#    spu_codes_area_set=set(spu_codes_area.tolist())#或者spu_codes_area.values
#    dataDf=dataDf[spu_codes.isin(spu_codes_area_set)]
#    return dataDf


# def edit_goods_spu():
#     ip="10.18.222.114"
#     userName="root"
#     password="123456"
#     db="hisense_ljm"
#     dbTable1="goods_spu_exter_final7"
# #    dbTable="goods_spu_search"
#     filterSql=''
#     dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
#     e=dataDf['spu_code']
    
#     port=3306
#     connect=pymysql.connect(host=ip,user=userName, password=password,port=port)
#     cursor = connect.cursor()
#     sqlDB='''USE '''+db
#     cursor.execute(sqlDB)
    
#     for i in range(3):
# #        sql1='''select spu_g_name,spu_category,spu_similar from '''+dbTable1+''' where spu_code='''+e[i]
# #        print(sql1)
# #        cursor.execute(sql1)
# #        f=cursor.fetchone()
#         sql2='''update '''+dbTable1+''' set goods_short_edit='''+"'"+f[0]+"'"+''',spu_name_synonym='''+"'"+f[0]+"'"+''',spu_name_similar='''+"'"+f[2]+"'"+''',spu_cate_third_edit='''+"'"+f[1]+"'"+''' where spu_code='''+e[i]  
#     #    print(sql2)
#         cursor.execute(sql2)
#     connect.commit()
#     connect.close()
    
    
    
    