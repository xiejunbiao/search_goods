# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:07:20 2020

@author: lijiangman
"""

# 电商领域的jieba分词设置初始化--语义纠错
# 商品名库，用来做意图识别
import sys,getopt,os
from searchhotmain.inition_config import config_result  # 数据库配置

import pickle
import pandas as pd
import pymysql  # python3

from searchmatch.analyzerbyme import jieba  # 从这里导入import，妙不可言，不用重复调用jieba了！
from jieba.posseg import dt
# from jieba import lcut_for_search
from pypinyin import lazy_pinyin  #不带声调

pathDir=os.path.dirname(__file__)
stw_all_path = os.path.join(pathDir, 'utils','stop_words_all_important_filter')
with open(stw_all_path, encoding='UTF-8') as f:
    stopwords = [line.strip() for line in f.readlines()]

# lcut_for_search怎么设置停用词和词库字典？
# 电商分词库--新上架商品的时候，要更新分词库


class DsSegLibrary:

    def __init__(self, path, config_result):
        self.path = path
        self.ip = config_result['ip']
        self.user = config_result['user']
        self.password = config_result['password']
        self.db = config_result['db']
        self.port = config_result['port']

        self.gss_tb = config_result['tb_goods_spu_search']

    def get_goods(self):
        #    filterSql=' WHERE goods_short_edit !="" GROUP BY goods_short_edit'
        # filterSql=' WHERE goods_short_edit !="" GROUP BY goods_short_edit_edit'
        ##读取商品简称和品牌
        dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)

        mysqlCmd = "SELECT gss.goods_short_edit FROM %s gss WHERE goods_short_edit !='' GROUP BY goods_short_edit" % (
            self.gss_tb)
        dataDf = pd.read_sql(mysqlCmd, dbc)
        goods_short_edit = dataDf['goods_short_edit']
        goods_short_edit = set(goods_short_edit)  ##商品简称集合

        mysqlCmd = "SELECT gss.goods_brand FROM %s gss WHERE goods_brand !=''  AND goods_brand !='--' GROUP BY goods_brand" % (
            self.gss_tb)
        dataDf = pd.read_sql(mysqlCmd, dbc)
        goods_brand = dataDf['goods_brand']
        goods_brand = set(goods_brand)  ##商品简称集合

        ##加上类别
        mysqlCmd = "SELECT gss.spu_cate_third_edit FROM %s gss WHERE spu_cate_third_edit !=''  AND spu_cate_third_edit !='--' GROUP BY spu_cate_third_edit" % (
            self.gss_tb)
        dataDf = pd.read_sql(mysqlCmd, dbc)
        spu_cate_third_edit = dataDf['spu_cate_third_edit']
        spu_cate_third_edit = set(spu_cate_third_edit)  ##商品简称集合

        # print(dataDf.columns)
        # 关闭数据库连接
        dbc.close()
        # dataDf=readMysqlPd(ip,userName,password,db,dbTable1,filterSql)

        goods_short_edit_set = goods_short_edit | goods_brand | spu_cate_third_edit
        self.goods_short_edit_set = goods_short_edit_set

    def get_goods_seg(self):

        goods_short_edit_set_seg = set()
        for i in self.goods_short_edit_set:
            i_tep = i.replace(',', ' ')  ##有的是逗号分隔
            i_tep = i_tep.replace('\n', '')  ##回车
            j_spl = i_tep.split(' ')
            # if '' in j_spl:##这个语句只能删除一次空！！！
            while '' in j_spl:
                j_spl.remove('')
            for j in j_spl:
                goods_short_edit_set_seg.add(j)

        self.goods_short_edit_set_seg = goods_short_edit_set_seg

    # 分词库生成，注意这里是叠加词库，如果有了错误的“的”，应该删除，必须加一层停用词过滤！！！！
    def generate_goods_seg_library(self):
        #    path_list=['百度停用词表.txt','哈工大停用词表.txt','四川大学机器智能实验室停用词库.txt','中文停用词表.txt']
        #    for i in path_list:
        with open(self.path, 'r', encoding='UTF-8') as lines:
            goods_words_list = lines.readlines()

        ##删除换行符
        goods_words_set = set()
        for i in goods_words_list:
            goods_words_set.add(i.replace('\n', ''))
        ##删除set中空元素 删除空
        if '' in goods_words_set:
            # print('yesss')
            goods_words_set.remove('')  # 删除空--防止文本最后一行是空

        goods_words_set_all = self.goods_short_edit_set_seg.union(goods_words_set)
        goods_words_set_all_filter=[i for i in goods_words_set_all if i not in stopwords]
        self.goods_words_set_all = goods_words_set_all_filter
        self.write2File2()

    def write2File2(self):
        set_len = len(self.goods_words_set_all)
        with open(self.path, 'w', encoding='UTF-8') as f:
            iterNum = 0  ##最后一行不用回车
            for i in self.goods_words_set_all:
                iterNum = iterNum + 1
                ##删除换行符
                ir = i.replace('\n', '')
                if iterNum < set_len:
                    f.write(ir + '\n')
                else:
                    f.write(ir)


def is_chinese(uchar):
    """判断一个unicode是否是汉字"""
    if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
        return True
    else:
        return False


def is_number(uchar):
    """判断一个unicode是否是数字"""
    if uchar >= u'\u0030' and uchar <= u'\u0039':
        return True
    else:
        return False


def is_alphabet(uchar):
    """判断一个unicode是否是英文字母"""
    if (uchar >= u'\u0041' and uchar <= u'\u005a') or (uchar >= u'\u0061' and uchar <= u'\u007a'):
        return True
    else:
        return False


def format_str(content):
    # 只保留汉字
    # content = unicode(content,'utf-8')
    content = str(content)
    content_str = ''
    for i in content:
        if is_chinese(i):
            content_str = content_str+i
    return content_str


def deleteSomeCixing(wordlist):
    wordlist_del=[]
    ##先保留汉字
    ##删除量词，袋，盒
    delCixing=['x','y','q','u','r','o','p','m','g','k','c','d','e']##https://blog.csdn.net/Yellow_python/article/details/83991967
    shou_dong_liangci=['支','只装','测试','果']
    shou_dong_Feiliangci=['米']#不能删除的
    # shou_dong_Feiliangci=['米','克']#不能删除的--500克，米--克貌似不用纠错，又不是商品

    for i in wordlist:
        
        word_han = format_str(i)##只保留汉字，删除非汉字
        if len(word_han)==0:
            continue
        ##过滤停用词
        if word_han in stopwords:
            continue
        
        if word_han in dt.word_tag_tab:
            cixing=dt.word_tag_tab[word_han]
        else:
            cixing='####'
            
        cxFirst=cixing[0]
        
        if (cxFirst not in delCixing) and (word_han not in shou_dong_liangci):  #助词的得开头是u
            wordlist_del.append(word_han)
        if word_han in shou_dong_Feiliangci:
            wordlist_del.append(word_han)
    
    return wordlist_del


class DsIndexWordsLib:
    
    def __init__(self, wordFreq, wordFreq_etl, wordFreq_notimportant, config_result):
        self.wordFreq=wordFreq    
        self.wordFreq_etl=wordFreq_etl
        self.wordFreq_notimportant=wordFreq_notimportant
        
        self.ip=config_result['ip']
        self.user=config_result['user']
        self.password=config_result['password']
        self.db=config_result['db']
        self.port=config_result['port']
        self.gss_tb=config_result['tb_goods_spu_search']
        
        
        # pass
    
    def getWordsSeg(self):
        
        mysqlCmd="SELECT spu_name,goods_short_edit\
        ,spu_cate_first,spu_cate_second,spu_cate_third,spu_cate_third_edit\
        ,goods_brand,shop_name from %s"  % (self.gss_tb)       
        dataDf = self.readMysqlmultiPd(mysqlCmd)
        self.wordlist_all=[]
        self.goods_short_edit_set=set()
        self.goods_cate_set=set()
        self.goods_brand_set=set()
        self.goods_short_edit_list=[]
        self.goods_cate_list=[]
        self.goods_brand_list=[]
        self.gbc_list=[]#简称、品牌、类别的list
        
        self.goods_cate_list_first=[]
        self.goods_cate_list_second=[]
        self.goods_cate_list_third=[]
        
        for i in range(len(dataDf)):

            """
            # 验婷--我要买XX，翻译成了我要麦XX，所以这里删除这个部分
            # 这里不删除字符，可以分词 # spu_name = re.sub('[（）, ()+:*×/【】=%-]','', dataDf['spu_name'].iloc[i])
            spu_name = dataDf['spu_name'].iloc[i]
            wordlist = jieba.lcut_for_search(spu_name)
            # 删除量词，袋，盒
            self.word_filter(wordlist)
            """
            sps = dataDf['goods_short_edit'].iloc[i]
            spu_tep=sps.replace(',',' ')  # 有的是逗号分隔
            wordlist=spu_tep.split(" ")  # 商品简称的list==商品简称或者类别，要增大次数
            wordlist=deleteSomeCixing(wordlist)
            self.wordlist_all += wordlist
            self.gbc_list += wordlist
            wordlistSet=set(wordlist)
            self.goods_short_edit_set=self.goods_short_edit_set.union(wordlistSet)#商品名库

            """类别"""
            sct = dataDf['spu_cate_first'].iloc[i].replace(',',' ')
            self.goods_cate_list_first += sct.split(" ")

            sct = dataDf['spu_cate_second'].iloc[i].replace(',',' ')
            self.goods_cate_list_second += sct.split(" ")
            

            scte = dataDf['spu_cate_third_edit'].iloc[i]
            spu_tep=scte.replace(',',' ')##有的是逗号分隔
            wordlist=spu_tep.split(" ")#商品简称的list
            sct = dataDf['spu_cate_third'].iloc[i].replace(',',' ')##加入原始第三类别---有的也是空格分开的，如调味 肉制品，所以需要split
            # scf = dataDf['spu_cate_first'].iloc[i].replace(',',' ')##加入原始第三类别---有的也是空格分开的，如调味 肉制品，所以需要split
            wordlist += sct.split(" ")
            self.goods_cate_list_third += sct.split(" ")
            
            wordlist=deleteSomeCixing(wordlist)
            self.wordlist_all += wordlist
            self.gbc_list += wordlist
            wordlistSet=set(wordlist)
            self.goods_cate_set=self.goods_cate_set.union(wordlistSet)


            gb = dataDf['goods_brand'].iloc[i]
            wordlist=gb.split(" ")#商品简称的list
            wordlist=deleteSomeCixing(wordlist)
            self.wordlist_all += wordlist
            # self.gbc_list += wordlist
            wordlistSet=set(wordlist)
            self.goods_brand_set=self.goods_brand_set.union(wordlistSet)#商品名库

            shop_name = dataDf['shop_name'].iloc[i]
            wordlist = jieba.lcut_for_search(shop_name)
            self.word_filter(wordlist)
            
        self.wordlist_pinyin=self.wordlist_to_pinyin(self.wordlist_all)
        self.whz_dict=self.wordlist_to_Freq(self.wordlist_all)#汉字重要性词频

    def wordlist_to_Freq(self, wordlist_all):#汉字重要性词频
        whz_dict={}
        for i in wordlist_all:
            if i in whz_dict:
                whz_dict[i] += 1
            else:
                whz_dict[i]=1
        
        
        # print(whz_dict['辣'])
        return whz_dict


    def word_filter(self, wordlist):
        wordlist = deleteSomeCixing(wordlist)
        self.wordlist_all += wordlist
        
    def get_pinyin_dict(self):
        wpy_dict={}#word pinyin
        for i,cnt in self.wordlist_pinyin.items():
            key_val=i.split('_')
            word_pin=key_val[0]
            word_han=key_val[1]
            if word_pin in wpy_dict:
                wpy_dict_tep={word_han:cnt}
                wpy_dict[word_pin].update(wpy_dict_tep)
            else:
                wpy_dict[word_pin]={word_han:cnt}

        ##商品简称和商品类别，要覆盖前面的key
        # goods_short_edit_cate_set=self.goods_short_edit_set.union(self.goods_cate_set)
        # goods_short_edit_cate_set.update(self.goods_brand_set)#加上品牌
        goods_short_edit_cate_dict={}
        goods_short_edit_cate_dict_Han_Freq={}#汉字词频
        # wordFreq=100#强制设置词频
        gbc_dict=self.wordlist_to_Freq(self.gbc_list)#汉字重要性词频
        self.gbc_dict=gbc_dict
        # print('asfas',gbc_dict['辣'])
        # print('锅',gbc_dict['锅'])
        for i in gbc_dict:

            # if i=='锅' or i=='进口':
                # print(i,gbc_dict[i])
            ##“辣”被错误标注成了goods_short_edit一次，可以通过词频进行清洗，
            ##同时将来可能选出同音词频率较大的，如“链子”“莲子”“帘子”
            if gbc_dict[i]>self.wordFreq_etl:
                

                word_pinyin = ''.join(lazy_pinyin(i))
                if word_pinyin in goods_short_edit_cate_dict:
                    goods_short_edit_cate_dict[word_pinyin].update({i:self.wordFreq})
                else:
                    goods_short_edit_cate_dict[word_pinyin]={i:self.wordFreq}
                goods_short_edit_cate_dict_Han_Freq[i]=self.wordFreq
        
        wpy_dict.update(goods_short_edit_cate_dict)
        self.wpy_dict=wpy_dict #拼音词频字典
        
        self.whz_dict.update(goods_short_edit_cate_dict_Han_Freq)#汉字词频
        
        ##词性处理，好吃，漂亮等作为不重要的词，降低词频
        keys=self.whz_dict.keys()
        for i in keys:
            if i in dt.word_tag_tab:
                cixing=dt.word_tag_tab[i]
            else:
                cixing='####'
            whzFre=self.whz_dict[i]
            if ((cixing[0] in ['a','v'])  and (whzFre<20)) or (i in ['辣', '好吃']):
                #在jieba分词的字典里面，是名词。不合理
                # D:\anaconda3\envs\test1\Lib\site-packages\jieba
                self.whz_dict[i]=self.wordFreq_notimportant
        
            
    def wordlist_to_pinyin(self, wordlist):
        wordlist_pinyin={}
        for i in wordlist:
            word_pinyin = ''.join(lazy_pinyin(i))
            key=word_pinyin+'_'+i
            if key in wordlist_pinyin:
                wordlist_pinyin[key] += 1
            else:
                wordlist_pinyin[key]=1
                
        return wordlist_pinyin
    
    def readMysqlmultiPd(self, mysqlCmd):
        # dbc = pymysql.connect(ip, user, password, db, port=3306)
        dbc = pymysql.connect(host=self.ip,user=self.user,password=self.password,db=self.db,port=self.port)
        dataDf = pd.read_sql(mysqlCmd, dbc)
        dbc.close()
        return dataDf


def initPickle():
    
    # cv=ConfigValue(confPath)
    # config_result=cv.get_config_values()##全局变量，运行一次，数据库配置。放到内存里面

    # -------
    # path="../searchMatchV2/utils/words_brand_goods_all.txt"#综合的品牌和商品词库

    path=os.path.join(pathDir, 'utils', 'words_brand_goods_all.txt')

    dsl=DsSegLibrary(path, config_result)
    dsl.get_goods()#商品简称和品牌
    dsl.get_goods_seg()#商品简称和品牌的分词
    dsl.generate_goods_seg_library()#商品分词词库#14026行
    
    # 电商反向索引词库的建立
    wordFreq_etl=2  # 锅只有5个，海飞丝只有3个    # 只针对简称和类别，过滤词频
    diwl=DsIndexWordsLib(wordFreq=100, wordFreq_etl=wordFreq_etl, wordFreq_notimportant=5,config_result=config_result)##电商反向索引词库
    diwl.getWordsSeg()    # 搜索引擎模式seg_list = jieba.cut_for_search(text)
    # a=diwl.wordlist_pinyin
    diwl.get_pinyin_dict()
    wpy_dict=diwl.wpy_dict##电商反向索引词库--拼音和汉字
    # goods_short_edit_set=diwl.goods_short_edit_set#商品名简称
    whz_dict=diwl.whz_dict
    # 需要转化成字典或者集合！！去重，直接用union集合
    # print(diwl.gbc_dict['口红'])
    ##基于规则的添加拼音纠错
    s={'wubaike':{'500g':100},'geli':{'蛤蜊':100},'gala':{'蛤蜊':100}
       ,'guofu':{'果脯':100},'jiashike':{'佳世客':100},'e':{'鹅':100}
       ,'shike':{'世客':100},'gao':{'搞':100},'16':{'石榴':100},'十六':{'石榴':100}}##因为语音转文字的时候，伍佰客是一个整体的词，300g不是整体

    for (key,value) in s.items():
        wpy_dict[key]=value
        
    # pathDir=os.path.dirname(__file__)
    path=os.path.join(pathDir, 'word_pkls')
    if not os.path.exists(path):
        os.makedirs(path)

    #持久化存储# pickle.dump(wp_dict, open('wp_dict.pk', 'wb'))  
    with open(os.path.join(path, 'wpy_dict.pkl'), 'wb') as f:
        pickle.dump(wpy_dict, f)
    
    #持久化存储# pickle.dump(wp_dict, open('wp_dict.pk', 'wb'))  
    with open(os.path.join(path, 'whz_dict.pkl'), 'wb') as f:
        pickle.dump(whz_dict, f)
    
    
    ##商品名称的命名实体映射字典构造
    ##注意：“玩具”既属于简称，也属于类别。不要被覆盖
    name_entity_dict={}#命名实体-##“辣”被错误标注成了goods_short_edit一次，可以通过词频进行清洗，
    for i in diwl.goods_short_edit_set:
        name_entity_dict[i]={'goods_short_edit'}
    
    
    """
    海飞丝只有3个    # 只针对简称和类别，过滤词频
    清洗词频小于10的
    儿童玩具--四个字的太难满足要求了，一票否决不合适---四个字的品牌，如“全棉时代”可以保留
    """
    goods_cate_set_etl=set()
    for i in diwl.goods_cate_set:
        if len(i)>=4:
            continue
        else:
            goods_cate_set_etl.add(i)
            
        if i in name_entity_dict:
            name_entity_dict[i].update({'spu_cate_third_edit'})
        else:
            name_entity_dict[i]={'spu_cate_third_edit'}

    ##"冷冻食品"，“食品”似乎也太苛刻，如果有的没有打上“食品”标签，则无法recall
            
    name_entity_dict_etl={key:value for (key,value) in name_entity_dict.items() if diwl.gbc_dict[key]>wordFreq_etl and len(key)<4}

    for i in diwl.goods_brand_set:
        if i in name_entity_dict_etl:
            name_entity_dict_etl[i].update({'goods_brand'})
        else:
            name_entity_dict_etl[i]={'goods_brand'}

    ##增加“无花果”实体
    # name_entity_dict_etl['无花果']={'goods_short_edit'} 已经在分词库里面增加了
    # 把分词库的所有字数少于4个字的都加入命名实体，当做重要的词！！
    for i in dsl.goods_words_set_all:
        if len(i)<4:
            if i not in name_entity_dict_etl:
                name_entity_dict_etl[i]={'goods_important'}
            else:
                pass
            ##已存在的key，就不更新了
            # if i in name_entity_dict_etl:
                # name_entity_dict_etl[i].update({'goods_important'})
            # else:
                # name_entity_dict_etl[i]={'goods_important'}

    # query='酸甜可口的苹果'#--可口竟然是一个品牌名，晕！暂时先去掉吧
    # 暂时删除进口，食品等，以后务必增加所有类别字段
    # ner_filter=['可口', '进口', '新鲜', '食品', '商品']
    # ner_filter=['可口', '进口', '新鲜', '商品', '用品']
    # ner_filter = ['可口', '进口', '新鲜', '商品', '用品', '饮品']
    # for i in ner_filter:
    #     if i in name_entity_dict_etl:
    #         del name_entity_dict_etl[i]
    # del name_entity_dict_etl['新鲜'] #暂时避免“新鲜的调料”，其实“新鲜”是一个字段
    # del name_entity_dict_etl['调味肉制品']
    # diwl.goods_cate_set.update({'可可制品'})#--其实是一个分类搜索模式啊，
    
    """APP中的第二级分类或第三级分类《火锅烧烤》，应该当做一个类别，不限制字数，属于商家预置类别，而不是第三级edit类别"""        
    for i in diwl.goods_cate_list_third:
        
        goods_cate_set_etl.add(i)            
        """第三级类别搜索"""
        if i in name_entity_dict_etl:
            name_entity_dict_etl[i].update({'cate_third_search'})
        else:
            name_entity_dict_etl[i]={'cate_third_search'}

    for i in diwl.goods_cate_list_first:
        
        goods_cate_set_etl.add(i)    
        """第一级类别搜索"""
        if i in name_entity_dict_etl:
            name_entity_dict_etl[i].update({'cate_first_search'})
        else:
            name_entity_dict_etl[i]={'cate_first_search'}
    
    for i in diwl.goods_cate_list_second:
        
        goods_cate_set_etl.add(i)    
        """第二级类别搜索"""
        if i in name_entity_dict_etl:
            name_entity_dict_etl[i].update({'cate_second_search'})
        else:
            name_entity_dict_etl[i]={'cate_second_search'}

    ner_filter = ['可口', '进口', '新鲜', '商品', '用品', '饮品']
    for i in ner_filter:
        if i in name_entity_dict_etl:
            del name_entity_dict_etl[i]

    # 进入类别搜索模式！！！
    with open(os.path.join(path, 'goods_cate_set.pkl'), 'wb') as f:
        pickle.dump(goods_cate_set_etl, f)

    with open(os.path.join(path, 'name_entity_dict.pkl'), 'wb') as f:
        pickle.dump(name_entity_dict_etl, f)
        
    # “支” “只装”都没有删除--会影响“纸”
    # print(wpy_dict['mi'])#米当做了量词？
    # print(wpy_dict['guangchang'])
    # print('苹果：',name_entity_dict_etl['苹果']) 
    # print('玩具：',name_entity_dict_etl['玩具'])
    # print('儿童：',name_entity_dict_etl['儿童'])
    # print('冷冻：',name_entity_dict_etl['冷冻'])
    # print(whz_dict['好吃'], whz_dict['辣'])
    ##好吃 词性是 对形容词，重要性要降低
    # word_han='好吃'
    # if word_han in dt.word_tag_tab:
    #     cixing=dt.word_tag_tab[word_han]
    # else:
    #     cixing='####'

    # print('function end : ner dict finished')
    return dsl
    # path_wpy_dict='../searchMatchV2/wpy_dict.pkl'
    # path_whz_dict='../searchMatchV2/whz_dict.pkl'
    # path_ner_dict='../searchMatchV2/name_entity_dict.pkl'
    # path_goods_cate='../searchMatchV2/goods_cate_set.pkl'
    # 
    # goods_cate_set=pickle.load(open(path_goods_cate,'rb'))#类别搜索

# 场景1.搜索与蛤蜊相关的，如“蛤蜊”“促销的蛤蜊”...,搜出的是“...格力”
# 场景2. 搞活动的盐-----识别成：搞活动的严
          # 秒杀盐-----------识别成：秒杀妍
# 场景3. 果脯-------------识别成：国府


if __name__=='__main__':
    # argv=sys.argv[1:]
    # confPath=para_set(argv)
    # confPath='D:\hisense\speechR\searchpkg\searchgoodshttp\server\config114.ini'#线上数据清洗
    # confPath='D:\hisense\speechR\searchpkg\searchgoodshttp\server\config99.ini'

    # dsl=initPickle()
    initPickle()
    pathDir=os.path.dirname(__file__)
    path=os.path.join(pathDir, 'word_pkls')

    path_ner_dict=os.path.join(path, 'name_entity_dict.pkl')
    ner_dict=pickle.load(open(path_ner_dict, 'rb'))
    path_ner_dict=os.path.join(path, 'goods_cate_set.pkl')
    goods_cate_set=pickle.load(open(path_ner_dict, 'rb'))

    whz_dict_path=os.path.join(path, 'whz_dict.pkl')
    whz_dict=pickle.load(open(whz_dict_path, 'rb'))    
    
    wpy_dict_path=os.path.join(path, 'wpy_dict.pkl')
    wpy_dict=pickle.load(open(wpy_dict_path, 'rb'))
    # test=wpy_dict["mai"]
    # print(test)
    print(ner_dict["饮品"])
