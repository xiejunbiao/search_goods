# -*- coding: utf-8 -*-
"""
Created on Mon May  4 10:47:27 2020

@author: lijiangman
"""
from searchhotmain.inition_config import config_result  # 数据库配置
from searchmatch.analyzerbyme import jieba  # 从这里导入import，妙不可言，不用重复调用jieba了！

import pymysql, os, logging, json
import pandas as pd
import traceback


""" 日志最小行数"""
line_num = 50000
""" 搜索热搜关键词数目下限"""
query_number = 200

"""
查询数据库的商品信息

"""


class NlpSeg:
    
    def __init__(self):
        
        pathDir=os.path.dirname(__file__)
        # path_word_seg = os.path.join(pathDir, 'utils','words_brand_goods_all.txt')
        # jieba.load_userdict(path_word_seg)
        
        stw_all_path = os.path.join(pathDir, 'utils','stop_words_all')
        with open(stw_all_path, encoding='UTF-8') as f:
            stopwordsA = [line.strip() for line in f.readlines()]
        
        yy_stopwords=[' ','啊','你','我','他','我想','我想买','想买','想要'
                      ,'要买','我想要','买点','来点','我想要买'
                      ,'给我','给我点','给','商品','我要'
                      ,'我要吃','要吃','想吃','我想吃','我想要吃'
                      ,'有没有','找']
        
        self.stopwords = stopwordsA + yy_stopwords
        

class SelectMysqlDatabase:
    
    # def __init__(self, config_result, line_num, query_number):
    def __init__(self):

        # self.query_number=1000##搜索热搜关键词数目下限
        # self.query_number=200##搜索热搜关键词数目下限
        self.query_number=query_number
        # self.path_search_log=config_result['log']##搜索日志目录
        self.path_log_std=config_result['log']##搜索日志目录
        self.path_log_std1=self.path_log_std+".1"
        self.path_log_stdmy=self.path_log_std+".cat"
        # if os.path.exists(path_log_std1):
        #     # path="/a/a" 命令行传参，日志路径
        #     # comm="cat stdout.log.bk stdout.log.bk1  > my_stdout.log"
        #     # comm="cat  %s  %s  >  %s" % (path_log_std, path_log_std1, path_log_stdmy)
        #     comm = "cat  %s  %s  >  %s" % (path_log_std1, path_log_std, path_log_stdmy)
        #     os.system(comm)
        #     self.path_search_log=path_log_stdmy
        # else:
        #     self.path_search_log=path_log_std##搜索日志目录

        self.line_num=line_num
        
        self.ip=config_result['ip']
        self.user=config_result['user']
        self.password=config_result['password']
        self.db=config_result['db']
        self.port=config_result['port']
        
        self.gs_tb=config_result['tb_goods_scope']##小区代码和spu代码对应关系
        self.gss_tb=config_result['tb_goods_spu_search']
        self.hsw_tb=config_result['tb_hot_search_words']
        self.sst_tb=config_result['tb_sensitive_words']  # 敏感词

        self.stopwords=NlpSeg().stopwords
        
        self.logger = logging.getLogger()    # initialize logging class
        self.logger.setLevel(logging.INFO)
        
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        self.logger.addHandler(console)

        """
        因为要初始化和定时任务统计热搜，所以必须实时读取小区编码，而非是固定的值
        """
        # self.area_codes=self.area_code_init()

    def area_code_init(self):
        """
        小区所有编码初始化
        """
        dbc = pymysql.connect(host=self.ip,user=self.user,password=self.password,db=self.db,port=self.port)

        mysqlCmd="SELECT distinct area_code FROM %s a " % (self.gs_tb)
        dataDf=pd.read_sql(mysqlCmd, dbc)
        area_codes=dataDf['area_code']

        mysqlCmd="SELECT distinct shop_code FROM %s a " % (self.gss_tb)
        dataDf=pd.read_sql(mysqlCmd, dbc)
        shop_codes=dataDf['shop_code']

        dbc.close()
        return area_codes, shop_codes

    def query_for_hotsearch_words(self, area_code):
        
        """
        查询预置热搜词mysql，按sort_number排序
        """
        
        dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
        a_col="a.hot_search_word"
        mysqlCmd="SELECT %s FROM %s a where a.area_code = '%s' order by a.sort_number" % (a_col, self.hsw_tb, area_code)
        dataDf=pd.read_sql(mysqlCmd, dbc)
        dbc.close()
        # goods_short_list=dataDf['hot_search_words']
        hot_search_words=list(dataDf['hot_search_word'])
        return hot_search_words

    def query_for_sensitive_words(self):

        dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
        a_col="a.autoid, a.sensitive_word"
        mysqlCmd="SELECT %s FROM %s a" % (a_col, self.sst_tb)
        dataDf=pd.read_sql(mysqlCmd, dbc)
        dbc.close()
        # goods_short_list=dataDf['hot_search_words']
        # hot_search_words=list(dataDf['sensitive_word'])
        # return hot_search_words
        return dataDf

    def get_shop_hot_goods(self, arg_dict):

        """
        读取店铺销量前5
        """
        area_code=arg_dict['areaCode']
        shop_code=arg_dict['shopCode']

        dbc = pymysql.connect(host=self.ip, user=self.user, password=self.password, db=self.db, port=self.port)
        a_col = "a.goods_short_edit"
        mysqlCmd = """  SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                    and b.area_code='%s' \
                    and a.goods_status=1 \
                    and a.shop_code='%s'
                    and a.goods_short_edit!=''\
                    ORDER BY a.sale_month_count DESC \
                    LIMIT 50 """ % (a_col, self.gss_tb, self.gs_tb, area_code, shop_code)

        # print(mysqlCmd)
        dataDf = pd.read_sql(mysqlCmd, dbc)
        dbc.close()
        schema = dataDf[['goods_short_edit']]
        schema_list=[]
        n_hot=5
        for index, row in schema.iterrows():
            i2=row['goods_short_edit'].replace(',',' ')
            i3=i2.strip()  # i3=i2.replace('\n','') 回车
            """
            对于建议suggestion，商品简称可以用空格建立索引
            schema_list.append({'goods_short':i3, 'spu_code':spu_code, 'goods_brand':goods_brand})
            对于热搜词来说，索引的是小区编码，输出的商品简称，只默认取出第一个即可
            """
            i_list=i3.split(' ')
            # while '' in i_list:##if '' in j_spl:##这个语句只能删除一次空！！！
            #     i_list.remove('')
            word=i_list[0]
            if word != '' and word not in schema_list:
                schema_list.append(word)
                if len(schema_list)>=n_hot:
                    break

        # results_hot_filter.append({"word": i, "hot": False})
        results_hot_filter=[{"word": i, "hot": False} for i in schema_list]
        return results_hot_filter

    def get_goods_schema(self, area_code):

        """
        读取商品简称和品牌---在线词
        """
        dbc = pymysql.connect(host=self.ip,user=self.user,password=self.password,db=self.db,port=self.port)
    
        # mysqlCmd="""
        # SELECT gss.goods_short_edit,gss.spu_code FROM %s gss 
        # WHERE goods_short_edit !='' GROUP BY goods_short_edit""" % (self.gss_tb)
        
        # mysqlCmd="""
        # SELECT gss.goods_short_edit, gss.goods_brand, gss.spu_code FROM %s gss 
        # WHERE goods_short_edit !='' """ % (self.gss_tb)
        # mysqlCmd="""
        # SELECT gss.goods_short_edit, gss.goods_brand, gss.spu_code FROM %s gss 
        #  """ % (self.gss_tb)
        
        a_col="a.spu_code, a.spu_name, a.goods_short_edit\
            ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            ,a.spu_cate_third, a.spu_cate_third_edit\
            ,a.sale_price, a.sale_month_count, a.shop_name, a.shop_code"
        
        if area_code=='-1':
            mysqlCmd="SELECT %s FROM %s a  \
                        where a.goods_status=1 " % (a_col, self.gss_tb)
        else:
            mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                        and b.area_code='%s' \
                        and a.goods_status=1 " % (a_col, self.gss_tb, self.gs_tb, area_code)
        
        dataDf=pd.read_sql(mysqlCmd, dbc)
        dbc.close()
        schema=dataDf[['goods_short_edit', 'goods_brand', 'spu_code', 'shop_code']]
        schema_list=self.get_schema_word(schema)##商品简称集合

        return schema_list
        
    def get_schema_word(self, schema):
        """
        获取字段的单词集合
        """
        # schema=set(schema)
        # schema_set=set()
        schema_list=[]
        for index, row in schema.iterrows():
            shop_code=row['shop_code']
            spu_code=row['spu_code']
            goods_brand=row['goods_brand']
            i2=row['goods_short_edit'].replace(',',' ')##有的是逗号分隔
            i3=i2.strip()#i3=i2.replace('\n','')##回车
            """
            对于建议suggestion，商品简称可以用空格建立索引
            schema_list.append({'goods_short':i3, 'spu_code':spu_code, 'goods_brand':goods_brand})
            对于热搜词来说，索引的是小区编码，输出的商品简称，只默认取出第一个即可
            """
            i_list=i3.split(' ')
            while '' in i_list:##if '' in j_spl:##这个语句只能删除一次空！！！
                i_list.remove('')
            if i_list:
                schema_list.append({'goods_short':i_list[0], 'spu_code':spu_code, 'goods_brand':goods_brand, 'shop_code':shop_code})
            # if len(schema_list)>1000:
                # break
            # j_list=i3.split(' ')
            # while '' in j_list:##if '' in j_spl:##这个语句只能删除一次空！！！
                # j_list.remove('')
            
            # for j in j_list:
                # schema_list.append({'goods_short':j, 'spu_code':spu_code, 'goods_brand':goods_brand})
        
        return schema_list

    def get_schema_list(self, area_code):

        """
        模拟热搜词的统计，
        
        判断商品名是否在用户的搜索词范围内，是否能避免“洗发水”？
        """
        dbc = pymysql.connect(host=self.ip,user=self.user,password=self.password,db=self.db,port=self.port)
    
        a_col="a.spu_code, a.spu_name, a.goods_short_edit\
            ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            ,a.spu_cate_third, a.spu_cate_third_edit\
            ,a.sale_price, a.sale_month_count, a.shop_name"    
        
        if area_code=='-1':
            mysqlCmd="SELECT %s FROM %s a  \
                        where a.goods_status=1 " % (a_col, self.gss_tb)
        else:
            mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                        and b.area_code='%s' \
                        and a.goods_status=1 " % (a_col, self.gss_tb, self.gs_tb, area_code)
        
        dataDf=pd.read_sql(mysqlCmd, dbc)
        dbc.close()

        schema=dataDf[['goods_short_edit', 'goods_brand', 'spu_code']]
        schema_list=[]
        for index, row in schema.iterrows():
            spu_code=row['spu_code']
            goods_brand=row['goods_brand']
            i2=row['goods_short_edit'].replace(',',' ')##有的是逗号分隔
            i3=i2.strip()#i3=i2.replace('\n','')##回车
            """
            对于建议suggestion，商品简称可以用空格建立索引
            schema_list.append({'goods_short':i3, 'spu_code':spu_code, 'goods_brand':goods_brand})
            对于热搜词来说，索引的是小区编码，输出的商品简称，只默认取出第一个即可
            """
            i_list=i3.split(' ')
            while '' in i_list:##if '' in j_spl:##这个语句只能删除一次空！！！
                i_list.remove('')
            if i_list:
                schema_list.append(i_list[0])
         
        result_seri=pd.value_counts(schema_list)
        # print(type(list(result)))
        
        return result_seri

    def get_schema_list_update_test(self, area_code):

        """
        模拟热搜词的统计，
        
        判断商品名是否在用户的搜索词范围内，是否能避免“洗发水”？
        """
        dbc = pymysql.connect(host=self.ip,user=self.user,password=self.password,db=self.db,port=self.port)
    
        a_col="a.spu_code, a.spu_name, a.goods_short_edit\
            ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            ,a.spu_cate_third, a.spu_cate_third_edit\
            ,a.sale_price, a.sale_month_count, a.shop_name"    
        
        if area_code=='-1':
            mysqlCmd="SELECT %s FROM %s a  \
                        where a.goods_status=1 " % (a_col, self.gss_tb)
        else:
            mysqlCmd="SELECT %s FROM %s a JOIN %s b ON a.spu_code=b.spu_code \
                        and b.area_code='%s' \
                        and a.goods_status=1 " % (a_col, self.gss_tb, self.gs_tb, area_code)
        
        dataDf=pd.read_sql(mysqlCmd, dbc)
        dbc.close()

        schema=dataDf[['goods_short_edit', 'goods_brand', 'spu_code']]
        schema_list=[]
        for index, row in schema.iterrows():
            spu_code=row['spu_code']
            goods_brand=row['goods_brand']
            i2=row['goods_short_edit'].replace(',',' ')##有的是逗号分隔
            i3=i2.strip()#i3=i2.replace('\n','')##回车
            """
            对于建议suggestion，商品简称可以用空格建立索引
            schema_list.append({'goods_short':i3, 'spu_code':spu_code, 'goods_brand':goods_brand})
            对于热搜词来说，索引的是小区编码，输出的商品简称，只默认取出第一个即可
            """
            i_list=i3.split(' ')
            while '' in i_list:##if '' in j_spl:##这个语句只能删除一次空！！！
                i_list.remove('')
            if i_list:
                schema_list.append(i_list[0])
        
        while '唇膏' in schema_list:
            schema_list.remove('唇膏')
        
        result_seri=pd.value_counts(schema_list)
        # print(type(list(result)))
        
        return result_seri
    
    def get_schema_list_schedule(self):
        """
        # 2020-04-22 09:24:52,455 - SEARCH_GOODS - INFO - input_dict-{"query": "苹果", "page": "1", "rows": "10", "areaCode": "A2020032600001", "shopCode": "-1", "sortMethod": "null"}
        # 2020-04-22 09:24:52,495 - SEARCH_GOODS - INFO - page_json-{"resultCode": "0", "msg": "操作成功", "data": {"searchRult": "3", "pageNum": 1, "pageSize": 10, "size": 0, "startRow": 0, "endRow": 0, "total": -1, "pages": 1, "list": [], "prePage": 0, "nextPage": 0, "isFirstPage": true, "isLastPage": true, "hasPreviousPage": false, "hasNextPage": false, "navigatePages": 8, "navigatepageNums": [1], "navigateFirstPage": 1, "navigateLastPage": 1, "firstPage": 1, "lastPage": 1}}
        
        定时更新搜索字段的统计数据
        分别统计用户的搜索关键词，然后用在线过滤即可啊
        """
        last_lines=self.get_last_lines()
        query_list_all=[]
        query_list_dict={}
        # areaCodeDefault='-1'
        if last_lines:
            # self.logger.info('log num is %s' % (self.line_num) )
            for ibyte in last_lines:
                i=ibyte.decode()
                # print(i,type(i))
                if (i.startswith("INFO")) or ("input_dict-" not in i):
                    continue
                else:
                    log_line_list=i.split("input_dict-")
                    query_str=log_line_list[-1]
                    # print(query_str, type(query_str))
                    query_dict=json.loads(query_str)
                    if ('areaCode' not in query_dict) or ('shop_code' not in query_dict) or ('query' not in query_dict):
                        continue
                    query=query_dict['query']
                    areaCode=query_dict['areaCode']
                    shopCode = query_dict['shop_code']
                    ac_sc=areaCode+"_"+shopCode
                    """搜索模式分词"""        
                    words=jieba.lcut_for_search(query, HMM=False)##分离出来了最长的“海信广场超市”，而同时没有“海信广场”
                    words_list = [w for w in words if w not in self.stopwords]
                    query_list_all=query_list_all+words_list
                    if ac_sc in query_list_dict:
                        query_list_dict[ac_sc] += words_list
                    else:
                        query_list_dict[ac_sc] = words_list
        else:
            self.logger.info('log num is less')
        
        result_seris={}
        # query_list_dict[areaCodeDefault]=query_list_all
        # sys.exit("aaa")
        """
        如果这个小区没有日志(或者日志list长度太短)，那么用所有的小区代替--旧的
        如果没有日志，则从小区商品里面直接抽取即可啊--新的
        """
        for ac_sc, query_list in query_list_dict.items():
            ac_sc_list=ac_sc.split('_')
            areaCode=ac_sc_list[0]
            shopCode = ac_sc_list[1]
            # result_seris[ac_sc] = pd.value_counts(query_list)
            result = pd.value_counts(query_list)
            if areaCode in result_seris:
                result_seris[areaCode].update({shopCode: result})
            else:
                result_seris[areaCode]={shopCode: result}

        # for i in area_codes:
        #     # print(i)##测试环境的小区编码，开头没有A，在日志中替换一下
        #     if i in query_list_dict:
        #         query_list=query_list_dict[i]
        #         result_seri=pd.value_counts(query_list)
        #         if len(result_seri)<self.query_number:
        #             result_seris[i]=pd.value_counts(query_list_dict[areaCodeDefault])
        #         else:
        #             result_seris[i]=result_seri
        #     else:
        #         query_list=query_list_dict[areaCodeDefault]
        #         result_seris[i]=pd.value_counts(query_list)
            
        return result_seris
        
    def get_last_lines(self):
        """
        get last line of a file
        :param fname: file name
        :return: last line or None for empty file
        """
        def get_fname():
            if os.path.exists(self.path_log_std1):
                # path="/a/a" 命令行传参，日志路径
                # comm="cat stdout.log.bk stdout.log.bk1  > my_stdout.log"
                # comm="cat  %s  %s  >  %s" % (path_log_std, path_log_std1, path_log_stdmy)
                comm = "cat  %s  %s  >  %s" % (self.path_log_std1, self.path_log_std, self.path_log_stdmy)##Linux
                # comm = "copy  %s + %s    %s" % (self.path_log_std1, self.path_log_std, self.path_log_stdmy)##Windows
                os.system(comm)
                # print(comm)
                path_search_log = self.path_log_stdmy
            else:
                path_search_log = self.path_log_std  ##搜索日志目录

            return path_search_log

        # fname=self.path_search_log
        fname=get_fname()
        line_num=self.line_num
        offset2 = -5000  # 设置偏移量
        last_lines=""
        try:
            filesize = os.path.getsize(fname)
            offset = filesize - 1000

            if filesize <= -offset2:
                return None
            
            with open(fname, 'rb') as f:  # 打开文件
                # 在文本文件中，没有使用b模式选项打开的文件，只允许从文件头开始,只能seek(offset,0)
                # first_line = f.readline()# 取第一行
                try:
                    f.seek(-offset, 2)  # seek(offset, 2)表示文件指针：从文件末尾(2)开始向前50个字符(-50)
                    last_lines = f.readlines()  # 读取文件指针范围内所有行
                except Exception as e:
                    print("error:", traceback.format_exc())
                    print("error:", e)

                # offset = -5000  # 设置偏移量
                # while True:
                # while -offset < filesize:
                #     """
                #     file.seek(off, whence=0)：从文件中移动off个操作标记（文件指针），正往结束方向移动，负往开始方向移动。
                #     如果设定了whence参数，就以whence设定的起始位为准，0代表从头开始，1代表当前位置，2代表文件最末尾位置。
                #     """
                #     # try:
                #     #     aa=f.seek(-1000000000000, 2)  # OSError: [Errno 22] Invalid argument
                #     # except Exception as e:
                #     #     print("error:", traceback.format_exc())
                #     #     print("error:", e)
                #     try:
                #         f.seek(offset, 2)  # seek(offset, 2)表示文件指针：从文件末尾(2)开始向前50个字符(-50)
                #     except Exception as e:
                #         print("error:", traceback.format_exc())
                #         print("error:", e)
                #         break
                #     # f.seek(offset, 2)  # seek(offset, 2)表示文件指针：从文件末尾(2)开始向前50个字符(-50)
                #     last_lines = f.readlines()  # 读取文件指针范围内所有行
                #     if len(last_lines) > line_num:  # 判断是否最后至少有两行，这样保证了最后一行是完整的
                #         # last_line = last_lines[-1]  # 取最后一行
                #         # print(type(last_lines), len(last_lines))
                #         break
                #     else:
                #         # 如果off为50时得到的readlines只有一行内容，那么不能保证最后一行是完整的
                #         # 所以off翻倍重新运行，直到readlines不止一行
                #         # offset *= 2
                #         # offset -= 10000
                #         offset -= 1000 ##减小防止偏移超出文件大小
                # # print('第一行为：\n' + first_line.decode())
                # # print('最后一行为：\n' + last_line.decode())
            if len(last_lines)>line_num:
                last_lines_sub=last_lines[1:line_num+1]##第一个元素可能不是完整的，因此从第一个元素开始
                self.logger.info("log line number is more than %s" % line_num)
                return last_lines_sub
            else:
                self.logger.info("log line number is less than %s" % line_num)
                return None

        except FileNotFoundError:
            # print(filename + ' not found!')
            self.logger.info(fname + ' not found!')

            return None
    

if __name__=='__main__':
        
    # from searchhotmain.inition_config import config_result##数据库配置

    # path_search_log=r'D:\hisense\projects\searchassociation\searchmain\cloudbrain-search-log\stdout99.log'
    # path_search_log=r'D:\hisense\projects\searchassociation\searchmain\cloudbrain-search-log\stdoutOnlineEnv.log'
    # line_num=3000
    # query_number=200

    # smd=SelectMysqlDatabase(config_result, line_num, query_number)
    smd = SelectMysqlDatabase()
    # smd.get_goods_schema(area_code)
    area_codes=['A2018012300015','-1']
    smd.get_last_lines()
    # for area_code in area_codes:
    #     goods=smd.query_for_hotsearch_words(area_code)
    #     ##“hot_search_words”的类型是varchar(40) NOT NULL，如果没有值，则我pandas读出来是空字符串
    #     print(goods)


#############
    # smd=SelectMysqlDatabase()
    # smd.get_goods_schema()
    # area_code='A2018012300015'
    # area_code='-1'
    
    # # schema_list=smd.get_goods_schema(area_code)
    # area_codes=smd.area_codes
    # result_seri=smd.get_schema_list(area_code)

    # result_seri_test=smd.get_schema_list_update_test(area_code)
    # last_lines=smd.get_last_lines()





