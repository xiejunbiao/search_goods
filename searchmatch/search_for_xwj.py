from searchmatch.search_and_revise import sge, qr, page_json_default, page_json_default_shop
import json, time
from searchmatch.sensor_log import search_event_log
# 把初始化的工作，都放到实例化，把实例化初始化放到函数外面！只运行一次！只实例化一次！！！！


def search_main(arg_dict):

    #注意：如果只实例化一次，则要注意self的变量不能一直增大！！！要赋值空
    #注意：如果只实例化一次，则要注意self的变量不能一直增大！！！要赋值空
    
    # 我自己写的搜索引擎，初始化的时候，不要初始化query，而是只初始化关于搜索引擎本身的参数！！
    # 因为搜索引擎是通用的算法框架，而非需要特例化？？？你看机器学习的所有算法，初始化的时候，
    # 都是配置一些跟算法有关的参数！！！！！！草
    
    # sge=SearchGoodsEgine(arg_dict=arg_dict)
    if len(arg_dict['query'])==0:
        if arg_dict['search_dim']=='1':
            return page_json_default()
        else:
            return page_json_default_shop()

    sge.keyword_extract(arg_dict)  # 全分词--注意空的情况！--增加拼音转汉字功能

    if len(sge.query_key_word_set)==0:
        if arg_dict['search_dim']=='1':
            return page_json_default()
        else:
            return page_json_default_shop()

    sge.named_entity_recognition()  # 命名实体识别

    arg_dict['query_ner_type']=sge.query_ner_type
    # print(arg_dict)
    page_dict=sge.open_index_spu_search(arg_dict)

    # a1=sge.query_ner_type
    # a2=sge.query_key_word_set
    # print(a1,a2,sep="\n")
    return page_dict


def search_query_revise(arg_dict):
    
    # 空请求
    if len(arg_dict['query'])==0:
        query_revise={"data": "", "resultCode": "0", "msg": "成功"}
        page_json=json.dumps(query_revise, ensure_ascii=False)  # False解决中文乱码
        return page_json
    else:
        query=arg_dict['query']
    
    # path='../searchMatchV2/wpy_dict.pkl'
    # path=os.path.join(pathDir,'wpy_dict.pkl')
    # wordFreq=30#语义纠正词库的词频阈值参数设置！！！    
    # wordFreq=5#佳沛
    # qr=QueryRevise(path, wordFreq)#参数初始化！！！软编码softcode
    
    qr.seg_and_change(query)#精准分词
    if qr.qBool:
        qr.query_pinyin_revise()#拼音修正
        # print(qr.query_pinyin)
    query=qr.query_p_r
    # 先分词，然后判断是否是pinyin字母，如果是拼音字母，则直接映射到电商词库字典的汉字
    # 如果是汉字，先映射到拼音，再映射到电商词库字典的汉字
    query_revise={"data": query, "resultCode": "0", "msg": "成功"}
    page_json=json.dumps(query_revise, ensure_ascii=False)  # False解决中文乱码

    return page_json


if __name__=='__main__':
    #鱼 --与 和于，为了匹配电商领域词库的时候，这个停用词不能去掉。。。    
    # print('aaaa')
    arg_dict = {}
    # arg_dict['query'] = "我想买漂亮美丽的苹果醋和苹果啊"
    # arg_dict['query'] = "我想买漂亮美丽的苹果啊"  ##苹果 梨 修改数据
    # arg_dict['query'] = "洗面奶"

    arg_dict={'query':"我想买漂亮的白沙河的美丽的苹果", 'areaCode':'-1', 'sort_method':'0'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"我想买漂亮", 'areaCode':'-1', 'sort_method':'0'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}
    # arg_dict={'query':"白啤", 'areaCode':'-1', 'sort_method':'0'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}
    # arg_dict={'query':"白酒", 'areaCode':'-1', 'sort_method':'0'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"白沙河", 'areaCode':'-1', 'sort_method':'0'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"白酒白啤", 'areaCode':'-1', 'sort_method':'0'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"岛", 'areaCode':'-1', 'sort_method':'0'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}


    arg_dict={'query':"甘肃茶叶", 'areaCode':'-1', 'sort_method':'0'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"明月海藻的洗发水", 'areaCode':'-1', 'sort_method':'0'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}


    arg_dict={'query':"我是谁想买王记鲜生的蟹棒", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"雀巢巧克力", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}


    arg_dict={'query':"巧克力绿茶味", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"绿茶冰激凌", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}
    #
    arg_dict={'query':"雀巢冰激凌", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"雀巢冰淇淋", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"我想买个雀巢的绿茶冰激凌", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}


    arg_dict={'query':"我想买鱼", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"榴莲", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"商品", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"淡黄的长裙", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"一果生活", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}

    arg_dict={'query':"海太", 'areaCode':'2020032600001', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1'}


    ownerCode="lijmTest"
    arg_dict={'query':"我要买苹果", 'areaCode':'A2018012300015', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1', 'ownerCode':ownerCode}
    arg_dict={'query':"我想买离职", 'areaCode':'A2018012300015', 'shop_code':'-1', 'sort_method':'-1'
                , 'search_dim':'1', 'rows':'10', 'page':'1', 'ownerCode':ownerCode}

    # 有的标注错误了，“牛奶饮品”，
    # 'query_ner_type': {'schemas_important': ['牛奶', '饮品'], 'schemas_not_important': ['牛奶饮品'], 'schemas_words_join': ['牛奶饮品']}}
    # 因此热搜词可能会出现“牛奶饮品”，但是不能搜出来。如果“牛奶饮品”是'schemas_important'，那么['牛奶', '饮品']是否都应该被忽略？
    # “牛奶饮品”长度大于等于4，因此不能当做'schemas_important'
    # 应该增加一个规则，如果goods_short_edit符合“牛奶饮品”，但是没有符合“牛奶”和“饮品”，也应该被搜出来。大胆尝试这个字段也中文分词？
    # 或者修改为“火锅烧烤”逻辑？不好
    # 或者暂时把“饮品”当做非重要词？yes  类别是否不建议作为必须的schemas_important？下次可以改进
    arg_dict = {'query': "牛奶饮品", 'areaCode': 'A2018012300015', 'shop_code': '-1', 'sort_method': '-1'
        , 'search_dim': '1', 'rows': '10', 'page': '1', 'ownerCode': ownerCode}


    # arg_dict = {'query': "部店", 'areaCode': '-1', 'shop_code': '-1', 'sort_method': '-1'
    #     , 'search_dim': '1', 'rows': '10', 'page': '1', 'ownerCode': ownerCode}



    # arg_dict = {'query': "永旺东部店", 'areaCode': 'A2018012300015', 'shop_code': '-1', 'sort_method': '-1'
    #     , 'search_dim': '1', 'rows': '10', 'page': '1', 'ownerCode': ownerCode}


    arg_dict = {'query': "旺东", 'areaCode': '-1', 'shop_code': '-1', 'sort_method': '-1'
        , 'search_dim': '1', 'rows': '10', 'page': '1', 'ownerCode': ownerCode}


    arg_dict = {'query': "果冻", 'areaCode': 'A20190121', 'shop_code': '-1', 'sort_method': '-1'
        , 'search_dim': '1', 'rows': '10', 'page': '1', 'ownerCode': ownerCode}


    arg_dict = {'query': "果冻", 'areaCode': 'A20190121', 'shop_code': '-1', 'sort_method': '-1'
        , 'search_dim': '2', 'rows': '10', 'page': '1', 'ownerCode': ownerCode}


    # arg_dict={'query':"雀巢的绿茶冰激凌", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}


    # arg_dict={'query':"我想买鱼", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}


    # arg_dict={'query':"我想买升仙", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"我想买王记鲜生", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"火锅烧烤", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'4'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"海信广场的精华露", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'4'
    #             , 'search_dim':'2', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"水果", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'4'
    #             , 'search_dim':'2', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"榴莲", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"洗发露", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"洗头膏", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"冰棍", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
                # , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"满减", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"梨", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'-1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"乳品烘焙", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'1'
    #             , 'search_dim':'2', 'rows':'10', 'page':'1'}
    # arg_dict={'query':"我想买漂亮的白沙河的美丽的苹果", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'3'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}

    # arg_dict={'query':"我想买漂亮", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}
    #
    # arg_dict={'query':"我想买苹果", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'3'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}


    # arg_dict={'query':"我想买30元苹果", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'1'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}


    # arg_dict={'query':"我想买啊", 'areaCode':'-1', 'shop_code':'356396279627776000', 'sort_method':'-1'
    #             , 'search_dim':'2', 'rows':'10', 'page':'1'}
    # arg_dict={'query':"我想买漂亮的白沙河的美丽的苹果", 'areaCode':'-1', 'shop_code':'-1', 'sort_method':'0'
    #             , 'search_dim':'1', 'rows':'10', 'page':'1'}
    # arg_dict={'query':"儿童玩具", 'areaCode':'-1', 'sort_method':'0'
    #             , 'search_dim':'2', 'rows':'10', 'page':'1'}

    # 白沙河 白啤 干白 白桐木--分词把“白”分出来了--能否通过词库解决

    # 导入分词库之后，白啤成功了，但是白酒好像还不行，明天试试分词

    t0=time.time()
    page_dict=search_main(arg_dict)
    if arg_dict['search_dim'] == '1':
        search_event_log(arg_dict, page_dict)

    t1=time.time()
    time_used = t1 - t0
    print("test:------\n", "Time consumed(s): ", time_used)
    print(page_dict)

    # pp=search_query_revise(arg_dict)
    # print(pp)


    
    
    
    
    
    
        