# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 10:37:20 2019

@author: wangpengpeng1
"""
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 13:53:58 2019

@author: wangpengpeng1
"""
#要放到D:\pyJ\searchMatchV0.1下 
import pymysql 
import pandas as pd
def readMysqlPd(ip,userName,password,db,dbTable,filterSql):
    db = pymysql.connect(ip,userName,password,db)
#    cursor = db.cursor()
    mysqlCmd="SELECT * FROM "+ dbTable + filterSql
    dataPd=pd.read_sql(mysqlCmd,db)
#    mysqlCmd="desc "+dbTable
#    cursor.execute(mysqlCmd)
#    print("describe:\n")
#    data = cursor.fetchall()
#    for row in data:
#        print("describe=", row)
#    cursor.execute("select count(*) from "+dbTable)
#    data=cursor.fetchall()
#    print(type(data))##tuple
#    print("rows: ",data[0][0])
#    mysqlCmd="SELECT * FROM "+ dbTable + " limit 2"
#    cursor.execute(mysqlCmd)
#    data = cursor.fetchall()
#    print("select:\n")
#    for row in data:
#        print("select=", row)
#    db.close()
    return dataPd
def read2Var(path):
    with open(path,'r',encoding='UTF-8') as lines:
        text_list=lines.readlines()
    
    return text_list
#def write2File4(path,f_set):
#    with open(path,'w',encoding='UTF-8') as f:
#        for i in range(len(f_set)):
#            f.write('%-30s$%-30s$%-12s$%-20s$%-50s$%-30s$%-20s$%-30s'%(spu_code[i],spu_name[i],spu_g_name[i],spu_category[i],spu_similar[i],shop_code[i],price_low[i],goods_status[i]))
#            f.write('\n')


ip="10.18.222.114"
userName="root"
password="123456"
db="hisense_ljm"
dbTable="goods_spu_exter_final5"      ##############要读取的数据库名字

##将表存到文本里
def db_write2text(): 
    filterSql=''
    dataDf=readMysqlPd(ip,userName,password,db,dbTable,filterSql)
    spu_code=dataDf['spu_code']
    spu_name=dataDf['spu_name']
    spu_g_name=dataDf['spu_g_name']
    spu_category=dataDf['spu_category']
    spu_similar=dataDf['spu_similar']
    shop_code=dataDf['shop_code']
    price_low=dataDf['price_low']
    goods_status=dataDf['goods_status']
    
    f_set=spu_similar
    path='./goods_spu_exter_final6'     ######################存到的txt名字 
    with open(path,'w',encoding='UTF-8') as f:
        for i in range(len(f_set)):
            f.write('%-30s$%-30s$%-12s$%-20s$%-50s$%-30s$%-20s$%-30s'%(spu_code[i],spu_name[i],spu_g_name[i],spu_category[i],spu_similar[i],shop_code[i],price_low[i],goods_status[i]))
            f.write('\n')

#    write2File4(path,f_set)    
##将txt文本导入到数据库
def text_write2db():
    dbTable="goods_spu_exter_final6" #################改参数，改为新建数据库名字
    port=3306
    good_final='./goods_spu_exter_final6'###############读取的txt文件名字
    good_name=read2Var(good_final)
    connect=pymysql.connect(host=ip,user=userName, password=password,port=port)
    cursor = connect.cursor()
    sqlDB='''USE '''+db
    cursor.execute(sqlDB)
    sql5='''drop table  if exists '''+dbTable
    cursor.execute(sql5)
    sql='''create table '''+dbTable+''' (good_id int PRIMARY key auto_increment,spu_code char(100) ,spu_name char(100), spu_g_name char(100), spu_category char(100),spu_similar char(100),shop_code varchar(40),price_low decimal(27,2),goods_status varchar(6))'''
    cursor.execute(sql)
    good_name_len=len(good_name)
    for i in range(good_name_len):
        i2=good_name[i].split('$')
        a0=i2[0].strip()
        a1=i2[1].strip()
        a2=i2[2].strip()
        a3=i2[3].strip()
        a4=i2[4].strip()
        a5=i2[5].strip()
        a6=i2[6].strip()
        a7=i2[7]
        a7=a7.replace('\n','')
        a7=a7.strip()
        sql1 = 'INSERT INTO ' +dbTable+' (spu_code,spu_name,spu_g_name,spu_category,spu_similar,shop_code,price_low,goods_status ) VALUES ('+"'"+a0+"'"+','+"'"+a1+"'"+","+"'"+a2+"'"+","+"'"+a3+"'"+","+"'"+a4+"'" +','+"'"+a5+"'"+","+"'"+a6+"'" +','+"'"+a7+"'"+   ')'
        cursor.execute(sql1)
    connect.commit()
    connect.close()
    
    


#db_write2text()
text_write2db()
            
            
            
            
            
            
   
