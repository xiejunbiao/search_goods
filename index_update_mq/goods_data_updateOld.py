import os, datetime
from whoosh.index import create_in, open_dir
from whoosh.writing import AsyncWriter
from searchhotmain.index_create import cshi
from whoosh.fields import Schema, TEXT, ID, NUMERIC, STORED
from searchmatch.index_update_cre import iu
# 搜索和热搜共用一个索引index同步
# pathDir = os.path.dirname(__file__)
# pathDir=os.path.dirname(os.getcwd())#上一级目录---当运行顶层目录的主程序的时候，这个命令导致输出的是顶层目录的上一层！
pathDir=os.path.abspath(os.path.dirname(os.path.dirname(__file__)))##这个才是文件所在的上级目录
# print(pathDir)
path_index_online = os.path.join(pathDir, 'searchhotmain', 'index_all', 'index_online', 'index')
path_index_spu_search = os.path.join(pathDir, 'searchmatch', 'index_all', 'index_spu_search', 'index')


def delete_search(area_code, goods_data_dict):
    path_index_spu_search_ac = path_index_spu_search + "_" + area_code
    if os.path.exists(path_index_spu_search_ac):
        ix_online = open_dir(path_index_spu_search_ac)
        # writer=ix_online.writer()
        writer = AsyncWriter(ix_online)
        writer.delete_by_term(fieldname='spu_code', text=goods_data_dict["spuCode"])
        # writer.delete_by_term('spu_code', spu_code)
        writer.commit(optimize=True)  # 还是会有两个seg文件
    else:
        pass


def delete_hot(area_code, goods_data_dict):
    path_index_online_ac = path_index_online + "_" + area_code
    if os.path.exists(path_index_online_ac):
        ix_online = open_dir(path_index_online_ac)
        # writer=ix_online.writer()
        writer = AsyncWriter(ix_online)
        writer.delete_by_term(fieldname='spu_code', text=goods_data_dict["spuCode"])
        writer.commit(optimize=True)  # 还是会有两个seg文件
        """
        更新最终的热搜过滤索引
        """
        cshi.create_index_hot_filter(area_code)
    else:
        pass


def add_search(area_code, goods_data_dict):
    path_index_spu_search_ac = path_index_spu_search + "_" + area_code
    if not os.path.exists(path_index_spu_search_ac):
        os.makedirs(path_index_spu_search_ac)
        ix = create_in(path_index_spu_search_ac,
                       iu.schema_spu_search)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
    else:
        ix=open_dir(path_index_spu_search_ac)

    writer = AsyncWriter(ix)
    updated_time_t = iu.getmtime_of_timestamp(str(goods_data_dict['updatedTimeDot']))
    writer.delete_by_term(fieldname='spu_code', text=goods_data_dict["spuCode"])
    # writer.commit(optimize=True)  # 还是会有两个seg文件

    writer.add_document(updated_time_dot=updated_time_t
                        , spu_code=goods_data_dict['spuCode']
                        , spu_name=goods_data_dict['spuName']
                        , shop_name=goods_data_dict['shopName']
                        , goods_brand=goods_data_dict['goodsBrand']
                        , goods_short_edit=goods_data_dict['goodsShortEdit']
                        , spu_cate_first=goods_data_dict['spuCateFirst']
                        , spu_cate_second=goods_data_dict['spuCateSecond']
                        , spu_cate_third=goods_data_dict['spuCateThird']
                        , spu_cate_third_edit=goods_data_dict['spuCateThirdEdit']
                        , shop_code=goods_data_dict['shopCode']
                        , sale_month_count=goods_data_dict['saleMonthCount']
                        , sale_price=goods_data_dict['salePrice'])

    writer.commit(optimize=True)  # 还是会有两个seg文件


def add_hot(area_code, goods_data_dict):
    path_index_online_ac = path_index_online + "_" + area_code
    if not os.path.exists(path_index_online_ac):
        os.makedirs(path_index_online_ac)
        ix = create_in(path_index_online_ac,
                       cshi.schema_online)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
    else:
        ix=open_dir(path_index_online_ac)

    writer = AsyncWriter(ix)
    # updated_time_t = iu.getmtime_of_timestamp(str(goods_data_dict['updatedTimeDot']))
    writer.delete_by_term(fieldname='spu_code', text=goods_data_dict["spuCode"])
    # writer.commit(optimize=True)  # 还是会有两个seg文件

    writer.add_document(spu_code=goods_data_dict['spuCode'],
                        goods_short=goods_data_dict['goodsShortEdit']
                        )
    writer.commit(optimize=True)  # 还是会有两个seg文件
    """
    更新最终的热搜过滤索引
    """
    cshi.create_index_hot_filter(area_code)


def update_hot(goods_data_dict, hot_or_search):

    if goods_data_dict["goodsStatus"] == '0':
        for area_code in goods_data_dict["areaCodes"]:
            if hot_or_search:
                delete_hot(area_code, goods_data_dict)
            else:
                delete_search(area_code, goods_data_dict)
    else:
        # 循环小区
        area_codes_old=goods_data_dict["areaCodesOld"]
        if len(area_codes_old)==0:
            pass
        else:
            for area_code in area_codes_old:
                if hot_or_search:
                    delete_hot(area_code, goods_data_dict)
                else:
                    delete_search(area_code, goods_data_dict)

        area_codes=goods_data_dict["areaCodes"]
        if len(area_codes)==0:
            pass
        else:
            for area_code in area_codes:
                if hot_or_search:
                    add_hot(area_code, goods_data_dict)
                else:
                    add_search(area_code, goods_data_dict)

        # 循环小区
        # if len(goods_data_dict["areaCodesOld"]) == 0:  # 验证一下刘尊彦传过来的是否是list类型？
        #     if not os.path.exists(path_index_online):
        #         os.makedirs(path_index_online)
        #     ix = create_in(path_index_online,
        #                    iu.schema_spu_search)  # 运行两遍之后，又出现了之前的错误！PermissionError:[Errno 13]Permission denied
        #
        #     writer = AsyncWriter(ix)
        #     updated_time_t = iu.getmtime_of_timestamp(str(goods_data_dict['updatedTimeDot']))
        #     writer.add_document(updated_time_dot=updated_time_t
        #                         , spu_code=goods_data_dict['spuCode']
        #                         , spu_name=goods_data_dict['spuName']
        #                         , shop_name=goods_data_dict['shopName']
        #                         , goods_short_edit=goods_data_dict['goodsShortEdit']
        #                         , spu_cate_third_edit=goods_data_dict['spuCateThirdEdit']
        #                         , shop_code=goods_data_dict['shopCode']
        #                         , sale_month_count=goods_data_dict['saleMonthCount']
        #                         , sale_price=goods_data_dict['salePrice'])
        #     writer.commit(optimize=True)  # 还是会有两个seg文件
        # else:

    #
    # ix_online = open_dir(path_index_online)
    # # writer=ix_online.writer()
    # writer = AsyncWriter(ix_online)
    # if goods_status == '0':
    #     writer.delete_by_term(fieldname='spu_code', text=spu_code)
    #     # writer.delete_by_term(fieldname='spu_code', text='skjdalkjfalj')##不存在也可以删除
    # else:
    #     writer.update_document(spu_code=spu_code, goods_short=goods_short,
    #                            goods_brand='测试品牌1')  # 为了满足小区配送的数据同步，先删除，再增加
    # writer.commit(optimize=True)


def update_status_online_index(goods_data_dict):
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
    update_hot(goods_data_dict, hot_or_search=True)
    update_hot(goods_data_dict, hot_or_search=False)


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
    for goods_data_dict in goods_data_list:
        update_status_online_index(goods_data_dict)

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
    update_index_online(spuDict)


if __name__=="__main__":

    # goods_data_update()

    pass







