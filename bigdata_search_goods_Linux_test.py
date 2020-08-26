# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 16:35:53 2019

@author: lijiangman
"""
import json, multiprocessing
import sys, datetime
import requests
def testaa():
    text_input="我想要白菜"
    #shopCode='315052326681550848'
    shopCode="-1"##默认所有小区
    #shopCode='314373898500284416'
    # areaCode='2018012300015'
    areaCode='2018012300005'##燕岛国际
    
    # areaCode='-1'
    #areaCode='2018012300014'
    #areaCode='181008'
    #areaCode='2018020800004'
    #spuCode='314377800163500032'
    sortMethod='-1'
    # url = "http://10.18.226.25:8870/searchgoods?"
    #8888ljm&areaCode=2018012300014&sortMethod=2&page=1&rows=50&searchKey="
    
    # url=url+"shopCode="+shopCode+"&areaCode="+areaCode+"&sortMethod="+sortMethod+"&page=1&rows=50&searchKey="+text_input
    # 
    # url="http://10.18.226.25:8872/searchQueryRevise?searchKey=升仙"
    
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchQueryRevise?shopCode=-1&areaCode=*2018012300014&sortMethod=1&page=1&rows=10&searchKey=升仙"
    r0 = requests.get(url)
    #print(r0)
    #print(r0.url)
    #print(r0.json())
    #page_json=search_main(text_input,1,50,areaCode,shopCode,sortMethod)
    page_dict=r0.json()
    #page_dict=json.loads(page_json)
    
    print(page_dict)
    
    # url="http://10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=1&page=1&rows=10&searchKey=洗发水"
    
    # r0 = requests.get(url)
    # page_dict=r0.json()
    # print(page_dict)
        
    # url="http://10.18.222.105:6611/bigdata-search/search/searchForGoods_etl_test?shopCode=-1&areaCode=-1&sortMethod=1&page=1&rows=10&searchKey=洗发水"
    
    # r0 = requests.get(url)
    # page_dict=r0.json()
    # print(page_dict)
        
    
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotIntro?areaCode=-1&fromType=2"
    # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=Axxxssssfd"
    
    r0 = requests.get(url)
    page_dict=r0.json()
    print(page_dict)
    
    
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotPreset"
    # url="http://10.18.226.25:6601/cloudbrain-search/search/searchHotPreset"
    spuDict={
        # "areaCode":"2018012300015",##如果不存在小区，则返回错误resultCode=1
        # "areaCode":"A20190425",##如果不存在小区，则返回错误resultCode=1

        "areaCode":"A2018012300015",
        # "areaCode":"-1",
        "hotPreset":['', '']
        # "hotPreset":['香蕉', '鸡蛋', '词穷了', '苹果', '洗发水', '洗发露', '', '']
        # "hotPreset":["1", None, None]##不能用None，要用空字符串""--否则whoosh的search会找不到key：goods_short！！！
        
    }
    aheaders={'Content-Type':'application/json'}
    result = requests.post(url, data = json.dumps(spuDict))##我省略了, headers=aheaders
    # result = requests.post(url, headers=aheaders, json = json.dumps(spuDict))##我省略了, headers=aheaders
    # print('hehe')
    # print(result)
    # print(result.headers)
    # text=result.text
    # print(text+'\n', type(text))
    result_json=result.text
    print(result_json)
    
    

        
def update_hot_search_test():

    """
    在__main__这里是全局变量，能被class里面的变量引用，因此放到一个函数里面
    goods_short_list竟然可以被类里面的变量调用！！！！
    """
    """
    更新索引的设计：
    我们最终的热搜词，要经过三个流程，热搜词统计，置顶，过滤
    热搜索引
    预置+热搜索引
    预置+热搜+过滤索引
    根据不同的更新，去更新相应的索引
    如果log日志变化，定时更新的时候，要经过所有1、2、3个流程
    如果置顶词变化，则经过2、3流程
    如果过滤词发生变化，则只经过3流程
    
    代码设计要根据上述，清晰地展示出上述思路！
    
    还有一种简单的方案：
    第一次初始化，要create生成
    log统计索引
    置顶索引
    过滤索引
    然后再读取索引生成最终的热搜索引
    
    然后定时全量create：log统计+置顶+过滤（与初始化相同）
    
    然后：
    支持置顶更新
    支持过滤更新
    
    每次更新，只需生成create一个索引和读取所有索引，走一遍流程即可
    
    
    相当于，先生成若干索引，再读取全部索引进行组合，生成最终索引。
    
    提炼出三个更新模块：初始化更新模块、置顶更新模块、过滤更新模块
    
    
    """
    print("-------update_hot_search_test")
    
    def postPreset(spuDict, url):
        # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotPreset"
        # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"

        
        headers={'Content-Type':'application/json'}

        # result = requests.post(url, data = json.dumps(spuDict))##我省略了, headers=aheaders
        result = requests.post(url, headers=headers, data=json.dumps(spuDict))##我省略了, headers=aheaders
        # print('hehe')
        # print(result)
        # print(result.headers)
        # text=result.text
        # print(text+'\n', type(text))
        # result_json=result.text##中文乱码
        result_dict=result.json()
        result_json=json.dumps(result_dict)
        # print(result_json, type(result_json), sep="\n")##中文乱码
        print(result_dict)

        
    def geturl(url):
        
        r0 = requests.get(url)
        page_dict=r0.json()
        print(page_dict)
        
    area_codes=['A2018012300015', '-1']    
    # cshi.create_search_words_init(area_codes)

    # cshi.search_hot_filter_test_all(area_codes)
    print("----search hot")

    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=A2018012300015"
    geturl(url)

    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=-1"
    geturl(url)

    """
    更新建立运营后台预置热搜词索引
    """
    

    print("----preset words")
    goods_short_list=['鸡蛋', '机器猫梨']
    areaCode='A2018012300015'
    spuDict={"areaCode":areaCode,"hotPreset":goods_short_list}
    # si=UpdateSearchHotIndex()
    # ushi.update_hot_preset_index(spuDict)
    # cshi.update_index_preset(spuDict)
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotPreset"
    
    postPreset(spuDict, url)

    # goods_short_list=['','','','']##空的情况下，词频置零
    goods_short_list=['鸡蛋', '机器猫', '苹果', '洗发水', '洗发露', '牛奶', '梨']
    areaCode='-1'
    spuDict={"areaCode":areaCode,"hotPreset":goods_short_list}
    # cshi.update_index_preset(spuDict)
    postPreset(spuDict, url)

    
    # cshi.search_hot_filter_test_all(area_codes)
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=A2018012300015"
    geturl(url)

    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=-1"
    geturl(url)    
    """
    更新上下架信息---只更新在线索引表
    把spu_code对应的小区，都应该更新一遍
    """
    
    print("----update goods data")

    goods_data_dict1={"goodsStatus":'1',
                "goodsShortEdit":'榴莲',
                "spuCode":'397227373373423616',##删除榴莲
                "areaCodes":['A2018012300015', '-1']}

    goods_data_dict2={"goodsStatus":'1',
                "goodsShortEdit":'机器猫梨',
                "spuCode":'19',    ##擦，终于找到原因了，update的时候，相同的spu_code覆盖了
                "areaCodes":['A2018012300015', '-1']}
    
    ##擦，终于找到原因了，update的时候，相同的spu_code覆盖了
    
    goods_data_list=[goods_data_dict1, goods_data_dict2]
    # goods_data_list=[goods_data_dict1,goods_data_dict1]
    spuDict={"goodsData":goods_data_list}
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"
    postPreset(spuDict, url)
    
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=A2018012300015"
    geturl(url)

    url="http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=-1"
    geturl(url)    

    # ushi.update_status_index_to_online(spuDict)
    # cshi.update_index_online(spuDict)
    # cshi.search_hot_filter_test_all(area_codes)
    # 10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey=水果
    # 10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=0&searchDim=2&page=1&rows=10&searchKey=水果


def update_hot_search_test2():
    """
    在__main__这里是全局变量，能被class里面的变量引用，因此放到一个函数里面
    goods_short_list竟然可以被类里面的变量调用！！！！
    """
    """
    更新索引的设计：
    我们最终的热搜词，要经过三个流程，热搜词统计，置顶，过滤
    热搜索引
    预置+热搜索引
    预置+热搜+过滤索引
    根据不同的更新，去更新相应的索引
    如果log日志变化，定时更新的时候，要经过所有1、2、3个流程
    如果置顶词变化，则经过2、3流程
    如果过滤词发生变化，则只经过3流程

    代码设计要根据上述，清晰地展示出上述思路！

    还有一种简单的方案：
    第一次初始化，要create生成
    log统计索引
    置顶索引
    过滤索引
    然后再读取索引生成最终的热搜索引

    然后定时全量create：log统计+置顶+过滤（与初始化相同）

    然后：
    支持置顶更新
    支持过滤更新

    每次更新，只需生成create一个索引和读取所有索引，走一遍流程即可


    相当于，先生成若干索引，再读取全部索引进行组合，生成最终索引。

    提炼出三个更新模块：初始化更新模块、置顶更新模块、过滤更新模块


    """
    print("-------update_hot_search_test")

    def postPreset(spuDict, url):
        # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotPreset"
        # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"

        headers = {'Content-Type': 'application/json'}

        # result = requests.post(url, data = json.dumps(spuDict))##我省略了, headers=aheaders
        result = requests.post(url, headers=headers, data=json.dumps(spuDict))  ##我省略了, headers=aheaders
        # result_json=result.text##中文乱码
        result_dict = result.json()
        result_json = json.dumps(result_dict)
        # print(result_json, type(result_json), sep="\n")##中文乱码
        print(result_dict)

    def geturl(url):
        r0 = requests.get(url)
        page_dict = r0.json()
        print(page_dict)

    area_codes = ['A2018012300015', '-1']
    # cshi.create_search_words_init(area_codes)

    # cshi.search_hot_filter_test_all(area_codes)
    print("----search hot")

    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=A2018012300015"
    geturl(url)

    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=-1"
    geturl(url)

    """
    更新建立运营后台预置热搜词索引
    """

    print("----preset words")
    goods_short_list = ['鸡蛋', '机器猫梨']
    areaCode = 'A2018012300015'
    spuDict = {"areaCode": areaCode, "hotPreset": goods_short_list}
    # si=UpdateSearchHotIndex()
    # ushi.update_hot_preset_index(spuDict)
    # cshi.update_index_preset(spuDict)
    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHotPreset"

    postPreset(spuDict, url)

    # goods_short_list=['','','','']##空的情况下，词频置零
    goods_short_list = ['鸡蛋', '机器猫', '苹果', '洗发水', '洗发露', '牛奶', '梨']
    areaCode = '-1'
    spuDict = {"areaCode": areaCode, "hotPreset": goods_short_list}
    postPreset(spuDict, url)

    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=A2018012300015"
    geturl(url)

    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=-1"
    geturl(url)
    """
    更新上下架信息---只更新在线索引表
    把spu_code对应的小区，都应该更新一遍
    """

    # print("----update goods data")
    ################################
    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey=榴莲"
    geturl(url)


    keys = ['spuName', 'shopName', 'goodsBrand', 'spuCateFirst', 'spuCateSecond', 'spuCateThird', 'spuCateThirdEdit',
            'shopCode', 'saleMonthCount', 'salePrice']
    time_loc = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # spu_code应该是唯一索引，add_document由于某个字段变化，如时间，会重复插入多个spu_code相同的数据！用update
    # 在模式中将更多字段标记为“唯一”将使每个字段
    # update_document
    # 稍微慢一点。
    #
    # 当您更新多个文档时，批量删除所有更改的文档，然后使用
    # add_document
    # 添加替换而不是使用
    # update_document.
    goods_data_dict1 = {"goodsStatus": '1',
                        "goodsShortEdit": '榴莲',
                        "spuCode": '407754603128160256',  ##删除榴莲
                        "areaCodes": ['A2018012300015', '-1'],
                        "areaCodesOld": ['A2018012300015Test', '-1'],
                        "updatedTimeDot": time_loc}

    goods_data_dict2 = {"goodsStatus": '1',
                        "goodsShortEdit": '榴莲 榴莲 你好测试',
                        "spuCode": '19',  ##擦，终于找到原因了，update的时候，相同的spu_code覆盖了
                        "areaCodes": ['A2018012300015', '-1'],
                        "areaCodesOld": ['A2018012300015', '-1Test'],
                        "updatedTimeDot": time_loc}

    for i in keys:
        goods_data_dict1[i] = '0'
        goods_data_dict2[i] = '0'

    ##擦，终于找到原因了，update的时候，相同的spu_code覆盖了

    goods_data_list = [goods_data_dict1, goods_data_dict2]
    # goods_data_list=[goods_data_dict1,goods_data_dict1]
    spuDict = {"goodsData": goods_data_list}

    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"
    postPreset(spuDict, url)

    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=A2018012300015"
    geturl(url)

    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchHot?areaCode=-1"
    geturl(url)


    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey=榴莲"
    geturl(url)
    ################################
    # ushi.update_status_index_to_online(spuDict)
    # cshi.update_index_online(spuDict)
    # cshi.search_hot_filter_test_all(area_codes)
    # 10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey=水果
    # 10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=0&searchDim=2&page=1&rows=10&searchKey=水果




def update_hot_search_test3():
    """
    在__main__这里是全局变量，能被class里面的变量引用，因此放到一个函数里面
    goods_short_list竟然可以被类里面的变量调用！！！！
    """
    # print("----update goods data")


    def postPreset(spuDict, url):
        # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotPreset"
        # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"

        headers = {'Content-Type': 'application/json'}

        # result = requests.post(url, data = json.dumps(spuDict))##我省略了, headers=aheaders
        result = requests.post(url, headers=headers, data=json.dumps(spuDict))  ##我省略了, headers=aheaders
        # result_json=result.text##中文乱码
        result_dict = result.json()
        # result_json = json.dumps(result_dict)
        # print(result_json, type(result_json), sep="\n")##中文乱码
        print(result_dict)

    def geturl(url):
        r0 = requests.get(url)
        page_dict = r0.json()
        print(page_dict)

    ################################
    # query="淡黄长裙"
    query="失败"
    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=A2018012300015&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey="+query
    geturl(url)

    keys = ['spuName', 'shopName', 'goodsBrand', 'spuCateFirst', 'spuCateSecond', 'spuCateThird', 'spuCateThirdEdit',
            'shopCode', 'saleMonthCount', 'salePrice']
    time_loc = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # spu_code应该是唯一索引，add_document由于某个字段变化，如时间，会重复插入多个spu_code相同的数据！用update
    # 在模式中将更多字段标记为“唯一”将使每个字段
    # update_document
    # 稍微慢一点。
    #
    # 当您更新多个文档时，批量删除所有更改的文档，然后使用
    # add_document
    # 添加替换而不是使用
    # update_document.
    goods_data_dict1 = {"goodsStatus": '0',
                        "goodsShortEdit": '榴莲 淡黄长裙 失败',
                        "spuCode": '407754603128160256',  ##删除榴莲
                        "areaCodes": ['A2018012300015', '-1'],
                        "areaCodesOld": ['A2018012300015Test', '-1'],
                        "updatedTimeDot": time_loc}

    goods_data_dict2 = {"goodsStatus": '1',
                        "goodsShortEdit": '榴莲 榴莲 你好测试',
                        "spuCode": '1999',  ##擦，终于找到原因了，update的时候，相同的spu_code覆盖了
                        "areaCodes": ['A2018012300015', '-1'],
                        "areaCodesOld": ['A2018012300015', '-1Test'],
                        "updatedTimeDot": time_loc}

    for i in keys:
        goods_data_dict1[i] = '0'
        goods_data_dict2[i] = '0'

    goods_data_list = [goods_data_dict1, goods_data_dict2]
    # goods_data_list=[goods_data_dict1,goods_data_dict1]
    spuDict = {"goodsData": goods_data_list}

    url_upd = "http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"
    postPreset(spuDict, url_upd)

    # url = "http://10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey="
    geturl(url)
    ################################
    # ushi.update_status_index_to_online(spuDict)
    # cshi.update_index_online(spuDict)
    # cshi.search_hot_filter_test_all(area_codes)
    # 10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey=水果
    # 10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=-1&sortMethod=0&searchDim=2&page=1&rows=10&searchKey=水果



def update_search_test(query):
    """
    在__main__这里是全局变量，能被class里面的变量引用，因此放到一个函数里面
    goods_short_list竟然可以被类里面的变量调用！！！！
    """
    print("----上下架之后测试搜索")

    def geturl(url):
        r0 = requests.get(url)
        page_dict = r0.json()
        print(page_dict)

    ################################
    url = "http://10.18.222.105:6601/cloudbrain-search/search/searchForGoods?shopCode=-1&areaCode=A2018012300015&sortMethod=-1&searchDim=1&page=1&rows=10&searchKey="+query
    geturl(url)

def freq_wirte_test():
    #快速写入测试

    def postPreset(spuDict, url):
        headers = {'Content-Type': 'application/json'}
        # result = requests.post(url, data = json.dumps(spuDict))##我省略了, headers=aheaders
        result = requests.post(url, headers=headers, data=json.dumps(spuDict))  ##我省略了, headers=aheaders
        # result_json=result.text##中文乱码
        result_dict = result.json()
        # result_json = json.dumps(result_dict)
        # print(result_json, type(result_json), sep="\n")##中文乱码
        print(result_dict)

    keys = ['spuName', 'shopName', 'goodsBrand', 'spuCateFirst', 'spuCateSecond', 'spuCateThird', 'spuCateThirdEdit',
            'shopCode', 'saleMonthCount', 'salePrice']
    time_loc = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    goods_data_dict1 = {"goodsStatus": '1',
                        "goodsShortEdit": '榴莲 淡黄长裙 失败',
                        "spuCode": '407754603128160256',  ##删除榴莲
                        "areaCodes": ['A2018012300015', '-1'],
                        "areaCodesOld": ['tt0','tt1','tt2','tt3','tt4','tt5','tt6','tt7', 'A2018012300015Test', '-1'],
                        "updatedTimeDot": time_loc}

    goods_data_dict2 = {"goodsStatus": '1',
                        "goodsShortEdit": '榴莲 榴莲 你好测试',
                        "spuCode": '1999',  ##擦，终于找到原因了，update的时候，相同的spu_code覆盖了
                        "areaCodes": ['A2018012300015', '-1'],
                        "areaCodesOld": ['A2018012300015', '-1Test'],
                        "updatedTimeDot": time_loc}

    for i in keys:
        goods_data_dict1[i] = '0'
        goods_data_dict2[i] = '0'

    goods_data_list = [goods_data_dict1, goods_data_dict1, goods_data_dict2]
    goods_data_list=goods_data_list*60
    spuDict = {"goodsData": goods_data_list}

    url_upd = "http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"

    def task1():
        for i in range(5):
            print(i)
            postPreset(spuDict, url_upd)
    def task2():
        for i in range(10):
            print(i)
            postPreset(spuDict, url_upd)
    def task3():
        for i in range(10):
            print(i)
            postPreset(spuDict, url_upd)

    # p1 = multiprocessing.Process(target=task1)  # multiprocessing.Process创建了子进程对象p1
    # p2 = multiprocessing.Process(target=task2)  # multiprocessing.Process创建了子进程对象p2
    # p3 = multiprocessing.Process(target=task3)  # multiprocessing.Process创建了子进程对象p3
    # p1.start()  # 子进程p1启动
    # p2.start()  # 子进程p2启动
    # p3.start()  # 子进程p3启动
    task1()
    print("finish")

def postPreset(spuDict, url):
    # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotPreset"
    # url="http://10.18.222.105:6601/cloudbrain-search/search/searchHotUpdate"

    headers = {'Content-Type': 'application/json'}

    # result = requests.post(url, data = json.dumps(spuDict))##我省略了, headers=aheaders
    result = requests.post(url, headers=headers, data=json.dumps(spuDict))  ##我省略了, headers=aheaders
    # result_json=result.text##中文乱码
    result_dict = result.json()
    # result_json = json.dumps(result_dict)
    # print(result_json, type(result_json), sep="\n")##中文乱码
    print(result_dict)


def mingan_test():
    spuDict={"searchKey": "我们热爱习近平啊"}
    url="http://10.18.222.105:6601/cloudbrain-search/search/searchForMinG"
    postPreset(spuDict=spuDict, url=url)


if __name__=="__main__":
    # testaa()
    # update_hot_search_test2()
    # update_hot_search_test3()
    # update_search_test(query="失败")
    # p1 = multiprocessing.Process(target=freq_wirte_test)  # multiprocessing.Process创建了子进程对象p1
    # p2 = multiprocessing.Process(target=freq_wirte_test)  # multiprocessing.Process创建了子进程对象p2
    # p3 = multiprocessing.Process(target=freq_wirte_test)  # multiprocessing.Process创建了子进程对象p3
    # p1.start()  # 子进程p1启动
    # p2.start()  # 子进程p2启动
    # p3.start()  # 子进程p3启动
    # freq_wirte_test()
    # update_search_test(query="失败")
    # update_search_test(query="商品")
    # update_search_test(query="号")
    mingan_test()
