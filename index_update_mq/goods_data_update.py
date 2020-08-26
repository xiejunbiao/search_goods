import os, datetime, time
from whoosh.index import create_in, open_dir
from whoosh.writing import AsyncWriter, BufferedWriter
from searchhotmain.index_create import cshi
from whoosh.fields import Schema, TEXT, ID, NUMERIC, STORED
from searchmatch.index_update_cre import iu
from whoosh.query import Every, Term
# 搜索和热搜共用一个索引index同步
# pathDir = os.path.dirname(__file__)
# pathDir=os.path.dirname(os.getcwd())#上一级目录---当运行顶层目录的主程序的时候，这个命令导致输出的是顶层目录的上一层！
pathDir=os.path.abspath(os.path.dirname(os.path.dirname(__file__)))##这个才是文件所在的上级目录
# print(pathDir)
path_index_online = os.path.join(pathDir, 'searchhotmain', 'index_all', 'index_online', 'index')
path_index_spu_search = os.path.join(pathDir, 'searchmatch', 'index_all', 'index_spu_search', 'index')

area_codes, shopCodes = cshi.get_area_codes()
area_code_quanwang_test = "A2020032600001"
area_code_quanwang_sc = "2020032600001"
# 34青岛准生产的智慧社区是A2020032600001，与生产环境的不一样啊！一个加A，一个不加A

# def delete_search(area_code, goods_dict_list):
#     path_index_spu_search_ac = path_index_spu_search + "_" + area_code
#     if os.path.exists(path_index_spu_search_ac):
#         ix_online = open_dir(path_index_spu_search_ac)
#         # writer=ix_online.writer()
#         writer = AsyncWriter(ix_online)
#         writer.delete_by_term(fieldname='spu_code', text=goods_data_dict["spuCode"])
#         # writer.delete_by_term('spu_code', spu_code)
#         writer.commit(optimize=True)  # 还是会有两个seg文件
#     else:
#         pass


def delete_hot(area_code, goods_dict_list, hot_or_search):

    def ws_writer():
        if hot_or_search:
            path_index_ac = path_index_online + "_" + area_code
        else:
            path_index_ac = path_index_spu_search + "_" + area_code

        # 当您更新多个文档时，批量删除所有更改的文档，然后使用
        # add_document    添加替换而不是使用    update_document.
        if os.path.exists(path_index_ac):
            ix_online = open_dir(path_index_ac)
            # writer=ix_online.writer()
            # writer = AsyncWriter(ix_online)
            writer = BufferedWriter(ix_online, period=600, limit=2000)
            for i in goods_dict_list:
                writer.delete_by_term(fieldname='spu_code', text=i["spuCode"])
            # writer.commit(optimize=True)  # 还是会有两个seg文件

            # Then you can use the writer to add and update documents
            # >> > writer.add_document(...)
            # >> > writer.add_document(...)
            # >> > writer.add_document(...)
            # Before the writer goes out of scope, call close() on it
            writer.close()

            """
            更新最终的热搜过滤索引
            """
            if hot_or_search:
                # cshi.create_index_hot_filter(area_code)
                results_hot_filter = cshi.create_index_hot_filter(area_code, shopCodes)  # 店铺---而且更新了索引
                cshi.update_index_online_filter(area_code, results_hot_filter)
        else:
            pass

    ct=0
    while ct<100:
        try:
            ws_writer()
            break
        except Exception as e:
            print(e)
            time.sleep(5)  ##second
            ct += 1


def add_search(area_code, goods_dict_list):

    def ws_writer():
        path_index_spu_search_ac = path_index_spu_search + "_" + area_code
        if not os.path.exists(path_index_spu_search_ac):
            os.makedirs(path_index_spu_search_ac)
            ix = create_in(path_index_spu_search_ac,
                           iu.schema_spu_search)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
        else:
            ix = open_dir(path_index_spu_search_ac)

        # 当您更新多个文档时，批量删除所有更改的文档，然后使用
        # add_document    添加替换而不是使用    update_document.
            # writer=ix_online.writer()
            # writer = AsyncWriter(ix_online)
        writer = BufferedWriter(ix, period=600, limit=2000)
        for i in goods_dict_list:
            updated_time_t = iu.getmtime_of_timestamp(str(i['updatedTimeDot']))
            writer.delete_by_term(fieldname='spu_code', text=i["spuCode"])
            writer.add_document(updated_time_dot=updated_time_t
                                , spu_code=i['spuCode']
                                , spu_name=i['spuName']
                                , shop_name=i['shopName']
                                , goods_brand=i['goodsBrand']
                                , goods_short_edit=i['goodsShortEdit']
                                , spu_cate_first=i['spuCateFirst']
                                , spu_cate_second=i['spuCateSecond']
                                , spu_cate_third=i['spuCateThird']
                                , spu_cate_third_edit=i['spuCateThirdEdit']
                                , shop_code=i['shopCode']
                                , sale_month_count=i['saleMonthCount']
                                , sale_price=i['salePrice'])

        # writer.commit(optimize=True)  # 还是会有两个seg文件

        # Then you can use the writer to add and update documents
        # >> > writer.add_document(...)
        # >> > writer.add_document(...)
        # >> > writer.add_document(...)
        # Before the writer goes out of scope, call close() on it
        writer.close()

    ct=0
    while ct<100:
        try:
            ws_writer()
            break
        except Exception as e:
            # print(e)
            time.sleep(5)  ##second
            ct += 1

    # path_index_spu_search_ac = path_index_spu_search + "_" + area_code
    # if not os.path.exists(path_index_spu_search_ac):
    #     os.makedirs(path_index_spu_search_ac)
    #     ix = create_in(path_index_spu_search_ac,
    #                    iu.schema_spu_search)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
    # else:
    #     ix=open_dir(path_index_spu_search_ac)

    # writer = AsyncWriter(ix)
    # updated_time_t = iu.getmtime_of_timestamp(str(goods_data_dict['updatedTimeDot']))
    # writer.delete_by_term(fieldname='spu_code', text=goods_data_dict["spuCode"])
    # # writer.commit(optimize=True)  # 还是会有两个seg文件
    #
    # writer.add_document(updated_time_dot=updated_time_t
    #                     , spu_code=goods_data_dict['spuCode']
    #                     , spu_name=goods_data_dict['spuName']
    #                     , shop_name=goods_data_dict['shopName']
    #                     , goods_brand=goods_data_dict['goodsBrand']
    #                     , goods_short_edit=goods_data_dict['goodsShortEdit']
    #                     , spu_cate_first=goods_data_dict['spuCateFirst']
    #                     , spu_cate_second=goods_data_dict['spuCateSecond']
    #                     , spu_cate_third=goods_data_dict['spuCateThird']
    #                     , spu_cate_third_edit=goods_data_dict['spuCateThirdEdit']
    #                     , shop_code=goods_data_dict['shopCode']
    #                     , sale_month_count=goods_data_dict['saleMonthCount']
    #                     , sale_price=goods_data_dict['salePrice'])

    # writer.commit(optimize=True)  # 还是会有两个seg文件


def add_hot(area_code, goods_dict_list):

    def ws_writer():
        path_index_online_ac = path_index_online + "_" + area_code
        if not os.path.exists(path_index_online_ac):
            os.makedirs(path_index_online_ac)
            ix = create_in(path_index_online_ac,
                           cshi.schema_online)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
        else:
            ix = open_dir(path_index_online_ac)

        # 当您更新多个文档时，批量删除所有更改的文档，然后使用
        # add_document    添加替换而不是使用    update_document.
            # writer=ix_online.writer()
            # writer = AsyncWriter(ix_online)
        writer = BufferedWriter(ix, period=600, limit=2000)
        for i in goods_dict_list:
            writer.delete_by_term(fieldname='spu_code', text=i["spuCode"])
            # writer.commit(optimize=True)  # 还是会有两个seg文件

            # writer.add_document(spu_code=i['spuCode'],
            #                     goods_short=i['goodsShortEdit'])
            writer.add_document(goods_short=i['goodsShortEdit'],
                                goods_brand=i['goodsBrand'],
                                shopCode=i['shopCode'],
                                spu_code=i['spuCode'])  # 增加商品上下架的服务。修改一个大bug，就是在线商品简称索引，忘了add添加spu_code

        # writer.commit(optimize=True)  # 还是会有两个seg文件

        # Then you can use the writer to add and update documents
        # >> > writer.add_document(...)
        # >> > writer.add_document(...)
        # >> > writer.add_document(...)
        # Before the writer goes out of scope, call close() on it
        writer.close()
        """
        更新最终的热搜过滤索引
        """
        # cshi.create_index_hot_filter(area_code)
        results_hot_filter = cshi.create_index_hot_filter(area_code, shopCodes)  # 全量店铺---而且更新了索引，而非新建索引
        cshi.update_index_online_filter(area_code, results_hot_filter)
    ct=0
    while ct<100:
        try:
            ws_writer()
            break
        except Exception as e:
            print(e)
            time.sleep(5)  ##second
            ct += 1

    # writer = AsyncWriter(ix)
    # updated_time_t = iu.getmtime_of_timestamp(str(goods_data_dict['updatedTimeDot']))
    # writer.delete_by_term(fieldname='spu_code', text=goods_data_dict["spuCode"])
    # # writer.commit(optimize=True)  # 还是会有两个seg文件
    #
    # writer.add_document(spu_code=goods_data_dict['spuCode'],
    #                     goods_short=goods_data_dict['goodsShortEdit']
    #                     )
    # writer.commit(optimize=True)  # 还是会有两个seg文件
    """
    更新最终的热搜过滤索引
    """
    # cshi.create_index_hot_filter(area_code)


def update_hot(area_goods_dict_old, area_goods_dict_new):

    # 旧小区-删除

    for ac, goods_dict_list in area_goods_dict_old.items():

        # 如果旧小区中有虚拟小区，则删除所有小区商品（全量删除）
        if ac in [area_code_quanwang_test, area_code_quanwang_sc]:
            for ac_qw in area_codes:
                delete_hot(ac_qw, goods_dict_list, hot_or_search=True)
                delete_hot(ac_qw, goods_dict_list, hot_or_search=False)
        else:
            delete_hot(ac, goods_dict_list, hot_or_search=True)
            delete_hot(ac, goods_dict_list, hot_or_search=False)

    # 新小区-添加或删除（）上架或下架
    for ac, goods_dict_list in area_goods_dict_new.items():
        goods_dict_list_0 = []
        goods_dict_list_1 = []

        for i in goods_dict_list:
            if i["goodsStatus"] == '0':
                goods_dict_list_0.append(i)
            else:
                goods_dict_list_1.append(i)

        if len(goods_dict_list_0)>0:
            # 如果新小区中有虚拟小区，且状态是下架，则只删除虚拟小区商品（增量删除）
            # delete_hot(ac, goods_dict_list_0, hot_or_search=True)
            # delete_hot(ac, goods_dict_list_0, hot_or_search=False)
            # 既然是下架商品，那么不管属于哪个小区，都应该删除！全量删除！--否则可能只删除了虚拟小区，但是其他小区也有此商品，因为消息同步接口可能没有传其他小区
            # 速度太慢，还是判断是否存在虚拟小区吧
            if ac in [area_code_quanwang_test, area_code_quanwang_sc]:
                for ac_qw in area_codes:
                    delete_hot(ac_qw, goods_dict_list_0, hot_or_search=True)
                    delete_hot(ac_qw, goods_dict_list_0, hot_or_search=False)
            else:
                delete_hot(ac, goods_dict_list_0, hot_or_search=True)
                delete_hot(ac, goods_dict_list_0, hot_or_search=False)

        if len(goods_dict_list_1)>0:
            # 如果新小区中有虚拟小区，且状态是上架，则增加所有小区商品（全量增加）--重复添加add会有问题吗？--会有问题，会重复，因此先delete再add
            if ac in [area_code_quanwang_test, area_code_quanwang_sc]:
                for ac_qw in area_codes:
                    add_hot(ac_qw, goods_dict_list_1)
                    add_search(ac_qw, goods_dict_list_1)
            else:
                add_hot(ac, goods_dict_list_1)
                add_search(ac, goods_dict_list_1)

# def update_status_online_index(area_goods_dict_old, area_goods_dict_new):
    # 增加商品搜索的索引同步
    # area_codes=goods_data_dict["areaCodes"]
    # spu_code=goods_data_dict["spuCode"]
    # goods_short=goods_data_dict["goodsShortEdit"]
    # goods_status=goods_data_dict["goodsStatus"]
    #
    # area_codes_old = goods_data_dict["areaCodesOld"]
    # spuName = goods_data_dict["spuName"]
    # spuName, goods_short_edit, goods_brand, spu_cate_first, spu_cate_second, spu_cate_third,
    # spu_cate_third_edit, sale_price, sale_month_count, shop_name, shop_code, updated_time_dot

    # for area_code in goods_data_dict["areaCodes"]:
        # path_index_online=self.path_index_online+"_"+area_code

    """
    注意检查是否存在index索引，不存在的话，跳过
    不存在的话，可能是新的小区，应该全量创建？
    首次上架，与非首次上架的区别。首次上架的时候，小区配送范围还没有。
    ==
    传参：spu_code，旧的小区范围areaCodesOld（删除），新的小区范围areaCodes（添加），上架1，下架0
    （1）	商品上架1，首次上架，areaCodesOld是空，非首次上架，areaCodesOld非空。
    空的时候，不用删除，直接添加（create创建索引），
    （2）	非空的时候，先删除再添加（open打开索引）。删除的时候，先判断是否存在索引目录，如果不存在，忽略。添加的时候，先判断是否存在索引目录，如果不存在，则创建。
    小区更新只有一种goods_status=1，相同的逻辑，空的时候，不用删除，直接添加，非空的时候，先删除再添加。
    （3）	商品下架0，根据新的areaCodes删除即可。先判断是否存在索引目录，如果不存在，忽略。
    （4）	每天mysql会有下架的delete删除操作吗？那edit字段怎么获得？？不应该定时删除下架，注释掉。先不传edit字段，我用定时更新同步。应该备份edit字段。

    """
    # update_hot(area_goods_dict_old, area_goods_dict_new)
    # update_hot(area_goods_dict_old, area_goods_dict_new, hot_or_search=False)


def update_index_online(spuDict):
    """
    更新在线索引
    """
    """
    更新上下架信息--在线索引
    删除delete
    更新update

    只关心下架，搜索查询下架商品是否存在于热搜，如果存在，直接重新计算一次热搜词？

    热搜索引（离线）是离线计算的，预留足够的数量1000，不需要更新。每次调用，取前10个，至少取10个
    只更新在线索引表，上架update，下架delete

    """
    """
    把spu_code对应的小区，都应该更新一遍
    """
    goods_data_list=spuDict['goodsData']
    area_goods_dict_old={}
    area_goods_dict_new={}
    # 以小区为单位，批量添加和批量删除！
    for iDict in goods_data_list:
        goods_dict={}
        for key, v in iDict.items():
            if key=='areaCodes' or key=='areaCodesOld':
                continue
            else:
                goods_dict[key]=v

        area_code_old=iDict['areaCodesOld']
        area_code_new=iDict['areaCodes']

        for ac in area_code_old:
            if ac in area_goods_dict_old:
                area_goods_dict_old[ac].append(goods_dict)
            else:
                area_goods_dict_old[ac]=[goods_dict]
        for ac in area_code_new:
            if ac in area_goods_dict_new:
                area_goods_dict_new[ac].append(goods_dict)
            else:
                area_goods_dict_new[ac]=[goods_dict]

    # 信我家智慧社区虚拟社区的商品是全网可见的。。。
    # area_code = 2020032600001
    # 商品上下架同步的时候，也要注意虚拟社区的商品，同步给所有小区
    # 删除虚拟小区的商品的时候，只删除此小区商品数据，但是增加虚拟小区的时候，全网增加！
    # for area_code_quanwang in [area_code_quanwang_test, area_code_quanwang_sc]:
    #
    #     if area_code_quanwang in area_goods_dict_old:
    #         for ac in area_codes:
    #             if ac in area_goods_dict_old:
    #                 area_goods_dict_old[ac] += area_goods_dict_old[area_code_quanwang]
    #             else:
    #                 area_goods_dict_old[ac] = area_goods_dict_old[area_code_quanwang]
    #
    #     if area_code_quanwang in area_goods_dict_new:
    #         for ac in area_codes:
    #             if ac in area_goods_dict_new:
    #                 area_goods_dict_new[ac] += area_goods_dict_new[area_code_quanwang]
    #             else:
    #                 area_goods_dict_new[ac] = area_goods_dict_new[area_code_quanwang]

    # for goods_data_dict in goods_data_list:
    # update_status_online_index(area_goods_dict_old, area_goods_dict_new)
    # print(area_goods_dict_new)

    update_hot(area_goods_dict_old, area_goods_dict_new)


def goods_data_update():
    # 商品数据状态同步更新
    print("----update goods data")
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
                        "goodsShortEdit": '榴莲 淡黄长裙',
                        # "spuCode": '407754603128160256',  ##删除榴莲
                        # "spuCode": '389772955241676800',  ##删除榴莲
                        "spuCode": '389772137306259456',
                        "areaCodes": ['A2018012300015', '-1', '2020032600001'],
                        "areaCodesOld": ['A2018012300015Test', '-1', '2020032600001'],
                        "updatedTimeDot": time_loc}

    goods_data_dict2 = {"goodsStatus": '1',
                        "goodsShortEdit": '榴莲 榴莲 你好测试',
                        "spuCode": '19999999999999',  ##擦，终于找到原因了，update的时候，相同的spu_code覆盖了
                        "areaCodes": ['A2018012300015', '-1', '2020032600001'],
                        "areaCodesOld": ['A2018012300015', '-1Test', '2020032600001'],
                        "updatedTimeDot": time_loc}

    for i in keys:
        goods_data_dict1[i] = '0'
        goods_data_dict2[i] = '0'

    goods_data_list = [goods_data_dict1, goods_data_dict2]
    spuDict = {"goodsData": goods_data_list}
    update_index_online(spuDict)


def search_online_test():
    path_index = cshi.path_index_online + "_A2018012300015"
    path_index=cshi.path_index_online+"_-1"
    path_index = path_index_spu_search + "_056"

    # path_index=cshi.path_index_hot_preset+"_A2018012300015"
    ix_online = open_dir(path_index)
    # 搜索查询所有goods_short字段的数据
    sc_online = ix_online.searcher()

    # qp=qparser.MultifieldParser(fieldnames=["goods_short"], schema=cshi.schema_online)
    # myquery=qp.parse("榴莲")
    myquery = Every()
    print('seg:', myquery)
    results = sc_online.search(myquery, limit=None)  # limit=None返回所有结果

    if len(results) > 0:
        for hit in results:
            print('hit:\n', hit)
            # print('content:\n', hit['content'])
            print('score:\n', hit.score)
    else:
        print('empty')

    # results_online_list=[str(i, encoding = "utf-8") for i in sc_online.lexicon("goods_short")]
    sc_online.close()

    return results


if __name__=="__main__":

    goods_data_update()
    search_online_test()
    # pass







