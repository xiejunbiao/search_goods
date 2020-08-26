## -*- coding: utf-8 -*-
#"""
#Created on Tue Dec 24 14:52:38 2019
#
#@author: lijiangman
#"""
#import pymysql 
#import pandas as pd
#
#ip="10.18.222.114"
#userName="root"
#password="123456"
#db="hisense_ljm"
#dbTable1="goods_spu_exter_final6"
#dbTable="goods_spu_search"
#filterSql=''
#dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)
#e=dataDf['spu_code']
#
#port=3306
#connect=pymysql.connect(host=ip,user=userName, password=password,port=port)
#cursor = connect.cursor()
#sqlDB='''USE '''+db
#cursor.execute(sqlDB)
#
#for i in range(len(e)):
#    sql1='''select spu_g_name,spu_category,spu_similar from '''+dbTable1+''' where spu_code='''+e[i]
#    print(sql1)
#    cursor.execute(sql1)
#    f=cursor.fetchone()
#    sql2='''update '''+dbTable+''' set goods_short_edit='''+"'"+f[0]+"'"+''',spu_name_synonym='''+"'"+f[0]+"'"+''',spu_name_similar='''+"'"+f[2]+"'"+''',spu_cate_third_edit='''+"'"+f[1]+"'"+''' where spu_code='''+e[i]  
##    print(sql2)
#    cursor.execute(sql2)
#connect.commit()
#connect.close()
#
