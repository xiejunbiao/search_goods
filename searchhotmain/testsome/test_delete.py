from searchhotmain.index_create import cshi
import os
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema,TEXT,ID,NUMERIC, FieldType
from whoosh.query import Every, Term
from whoosh.writing import AsyncWriter

index_hot_filter = cshi.path_index_hot_filter + "_" + "test1112223"


def test_delete():
    res={"shop_c1": [{"word": "鸡蛋", "hot": True}, {"word": "机器猫梨", "hot": True}, {"word": "呵呵呵你好帅", "hot": True}],
         "shop_c2": [{"word": "鸡蛋", "hot": True}, {"word": "机器猫梨", "hot": True}, {"word": "呵呵呵你好帅", "hot": True}]}

    cshi.create_index_online_filter("test1112223", res)
    # delete_by_query(q, searcher=None)
    ix = open_dir(index_hot_filter)
    writer = AsyncWriter(ix)
    query = Term("shopCode", "shop_c1")
    writer.delete_by_query(query)

    writer.add_document(goods_short="test_gs", search_frequency=10, shopCode="test_he", spu_code='12121')
    writer.add_document(goods_short="test_gs", search_frequency=10, shopCode="test_he", spu_code='12122')
    writer.add_document(goods_short="test_gs", search_frequency=10, shopCode="test_he", spu_code='12123')

    writer.commit()  # 自动关闭

    ix = open_dir(index_hot_filter)
    writer = AsyncWriter(ix)
    query = Term("spu_code", "12122") # 两种方法都可以删除，但是delete和add必须一次性提交commit之后，才能下一组delete和add
    writer.delete_by_query(query)
    # writer.delete_by_term("spu_code", "12122")
    writer.add_document(goods_short="test_gs", search_frequency=10, shopCode="test_he", spu_code='12122')#会重复增加！！！因此要先删除

    writer.commit()  # 自动关闭


test_delete()
if os.path.exists(index_hot_filter):
    sc_hot = open_dir(index_hot_filter).searcher()
    # results_hot_list=list(sc_hot.lexicon("goods_short"))##好像是默认字母顺序
    # results_hot_list=[str(b, encoding = "utf-8") for b in results_hot_list]
    myquery = Every()
    # filter = Term("shopCode", arg_dict['shopCode'])
    results_hot = sc_hot.search(myquery, limit=None)  # limit=None返回所有结果
    # results_hot_list = [json.loads(i['goods_short']) for i in results_hot]
    # results_hot_list=[i['search_frequency'] for i in results_hot]
    # elapsed = time.time() - start
    # print("Time used:%s s" % (elapsed))  # 时间长？？60ms

    for hit in results_hot:
        print(hit)
    sc_hot.close()  # 记得close啊sc_hot。close，而且要判断，是否存在dir，不存在，则返回空list
