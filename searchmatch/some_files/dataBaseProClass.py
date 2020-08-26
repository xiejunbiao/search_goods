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


class sqlsearchPre预处理，根据排序方式，不用构造销量和价格字典！
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
        
    if sortMethod=='null':##matchScore
        
        a_col="a.spu_code, a.spu_name, a.goods_short\
        ,a.goods_brand, a.spu_sale_point, a.spu_cate_first\
        ,a.spu_cate_second, a.spu_cate_third, a.spu_cate_third_edit\
        ,a.shop_name"
    else:
        a_col="a.spu_code, a.spu_name, a.goods_short\
        ,a.goods_brand, a.spu_sale_point, a.spu_cate_first\
        ,a.spu_cate_second, a.spu_cate_third, a.spu_cate_third_edit\
        ,a.sale_price, a.sale_month_count, a.shop_name"
    
    
        
    
    # 'spu_code', 'spu_name', 'goods_short', 'goods_short_edit',
    #    'spu_name_synonym', 'spu_name_similar', 'goods_brand',
    #    'goods_brand_edit', 'spu_sale_point', 'spu_cate_first',
    #    'spu_cate_first_edit', 'spu_cate_second', 'spu_cate_second_edit',
    #    'spu_cate_third', 'spu_cate_third_edit', 'sale_price',
    #    'sale_month_count', 'spu_ms', 'spu_yhq', 'shop_name', 'shop_code',
    #    'goods_status', 'spu_info_all', 'spu_info_all_edit'
    mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
            and b.area_code='%s' \
            and a.goods_status=1 \
            and a.shop_code='%s' \
            and (a.spu_info_all REGEXP '%s' \
            or a.spu_info_all_edit REGEXP '%s' \
            or a.spu_name REGEXP '%s')" % (a_col, gss_tb, gs_tb, area_code, shop_code, keyword_reg, keyword_reg, keyword_reg)
        
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


# ##商铺过滤，用mysql    
# def readGoodsSearchOld(ip,dbTable1,dbTable2,areaCode,shopCode,db,words_cut_all,userName,password):
    
# ##mysql正则包含关键词
#     words_cut_all=list(words_cut_all)

#     wl=len(words_cut_all)
#     if wl==0:
#         df = pd.DataFrame()
#         return df##空的dataFrame
    
#     if wl==1:
#         keyword_reg=words_cut_all[0]

#     if wl>1:
#         keyword_reg=words_cut_all[0]
#         for i in range(1,wl):
#             keyword_reg=keyword_reg+"|"+words_cut_all[i]
# #    SELECT * FROM goods_spu WHERE spu_name REGEXP  '苹果|葡萄|莱西'  AND goods_status=01
# #    if shopCode=='8888ljm':##没有shopcode
# ###################
# #    spu_info_all应该替换为spu_info_all_edit
#     filterSql=""
#     if shopCode=='-1':##没有shopcode
# ##        filterSql=" where goods_status=01"#过滤sql语句---#在线商品01--周生伟改为了1
# #        filterSql=" where goods_status=1 "+ " and spu_name REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---#在线商品01--周生伟改为了1
# #        filterSql=" where goods_status=1 "+ " and spu_info_all REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---#在线商品01--周生伟改为了1
# #        SELECT * FROM goods_spu_search WHERE goods_status=1  AND (spu_info_all REGEXP  '酸奶' OR goods_short REGEXP '酸奶')
# #        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"+" or "+"goods_short REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
# #        filterSql=" where goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
#         ##加入spu_info_all
# #        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
#         ##为了能够搜到网易严选的未清洗数据，加入spu_name
# #        filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
#         filterSql=" where goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'"  +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'" +" )"#过滤sql语句---#在线商品1

#     else:
# ##        filterSql=" where shop_code="+shopCode+" and goods_status=01"#过滤sql语句---商铺代码#在线商品    
# #        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and spu_name REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---商铺代码#在线商品    
# #        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and spu_info_all REGEXP " +"'"+keyword_reg+"'"#过滤sql语句---商铺代码#在线商品    

# #        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"+" or "+"goods_short REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
# #        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
# #        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品01--周生伟改为了1
#         ##加入spu_info_all
# #        filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'"  +" or "+"goods_short_edit REGEXP "+"'"+keyword_reg+"'"+" )"#过滤sql语句---#在线商品1
#         filterSql=" where shop_code="+shopCode+" and goods_status=1 "+ " and (spu_info_all REGEXP " +"'"+keyword_reg+"'"  +" or " + "spu_info_all_edit REGEXP " +"'"+keyword_reg+"'" +" or "+"spu_name REGEXP "+"'"+keyword_reg+"'" +" )"#过滤sql语句---#在线商品1

#         ###################    
# #    dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
# #    spu_codes=dataDf['spu_code']
#     ##小区代码
# #    dbTable2="goods_scope"##小区代码和spu代码对应关系
#     if areaCode=='-1':
#         filterSql=filterSql+" and spu_code in (select spu_code from goods_scope)"
#     else:
#         filterSql=filterSql+" and spu_code in (select spu_code from goods_scope where AREA_CODE=" + areaCode + ")"
#     dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
# #    dataDf_scope=readMysqlPd(ip,userName,password,db,dbTable2,filterSql)
    
# #    spu_codes_area=dataDf_scope['spu_code']
# #    spu_codes_area_set=set(spu_codes_area.tolist())#或者spu_codes_area.values
# #    dataDf=dataDf[spu_codes.isin(spu_codes_area_set)]
#     return dataDf


# ##输出pandas结构
# def readMysqlPd(ip,userName,password,db,dbTable,filterSql):
#     # 打开数据库连接
#     db = pymysql.connect(ip,userName,password,db)
# #    db = pymysql.connect("10.18.226.56","root","123456","hisense" )
# #    print(db)
#     # 使用 cursor() 方法创建一个游标对象 cursor
#     cursor = db.cursor()
#     # 使用 execute()  方法执行 SQL 查询 
# #    cursor.execute("SELECT VERSION()")
# #    mysqlCmd="SELECT interface FROM adapter_record" 
# #    mysqlCmd="SELECT * FROM adapter_record" 
#     mysqlCmd="SELECT * FROM "+ dbTable + filterSql
#     # print(mysqlCmd)
# #    SELECT DISTINCT LEFT(order_no,8) FROM hjmall_order  WHERE is_pay=1
#     ##
#     dataPd=pd.read_sql(mysqlCmd,db)
#     ##字段描述
#     mysqlCmd="desc "+dbTable
#     cursor.execute(mysqlCmd)
#     # 使用 fetchone() 方法获取单条数据.  data = cursor.fetchone()
#     # 使用 fetchall() 方法获取所有数据.
# #    print("describe:\n")
# #    data = cursor.fetchall()
# #    for row in data:
# #        interf=row[0]
# #        print("describe=", row)
#     ##
# #    统计多少行。。。。
#     cursor.execute("select count(*) from "+dbTable)
# #    data=cursor.fetchall()
# #    print(type(data))##tuple
# #    print("rows: ",data[0][0])
#     ##
#     mysqlCmd="SELECT * FROM "+ dbTable + " limit 2"
#     cursor.execute(mysqlCmd)
# #    data = cursor.fetchall()
# #    print("select:\n")
# #    for row in data:
# #        print("select=", row)
#     # 关闭数据库连接
#     db.close()
#     return dataPd
