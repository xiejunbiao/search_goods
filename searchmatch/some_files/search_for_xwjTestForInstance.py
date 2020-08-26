#
#
#
#
#
#
# # 灵感来源于《大数据架构》这本书。通常情况下，只需要一个搜索器的实例instance就行！
# # 只有在索引有所更新的时候，才需要重新打开新的搜索器实例！
#
# # 检查文件是否更新？？
#
# # 在此我测试重新打开实例和重复利用现有实例的耗时！
# # 实例化类的时候初始化pkl文件
# # 模仿jieba分词的load分词库啊
# # default_logger.debug("Building prefix dict from %s ..." % (abs_path or 'the default dictionary'))
# # jieba 如何判断初始化分词字典???好像只在第一次运行的时候才load啊？？不会在更新的时候自动运行load？
#
#
# import sys
# import time
# sys.path.append("../") ## 会从上一层目录找到一个package
# from searchMatchV2.search_and_reviseTest import SearchGoodsEgine, QueryRevise
# import json
#
#
#
#
# # #############################
# #空请求
# def nullReturn(sge):
#     searchRult="3"##没有结果
#     page_json=sge.page_json_default(searchRult)
#     return page_json
#
#
# indexdir='../searchMatchV2/wp_dict.pkl'
# sge=SearchGoodsEgine(indexdir)#实例化搜索引擎---放到外面明显时间很短
#
# 把初始化的工作，都放到实例化，把实例化初始化放到函数外面！只运行一次！只实例化一次！！！！
# 把初始化的工作，都放到实例化，把实例化初始化放到函数外面！只运行一次！只实例化一次！！！！
# 把初始化的工作，都放到实例化，把实例化初始化放到函数外面！只运行一次！只实例化一次！！！！
#
# def search_main(arg_dict):
#
#     # 我自己写的搜索引擎，初始化的时候，不要初始化query，而是只初始化关于搜索引擎本身的参数！！
#     # 因为搜索引擎是通用的算法框架，而非需要特例化？？？你看机器学习的所有算法，初始化的时候，
#     # 都是配置一些跟算法有关的参数！！！！！！草
#
#     # indexdir='../searchMatchV2/wp_dict.pkl'
#     # sge=SearchGoodsEgine(indexdir)#实例化搜索引擎
#
#     whoosh 我如果新建了一个实例，那么下次再新建的话，
#     要关闭。如果只新建一次实例，就可以一直用下去！
#
#     if clean_index:
#         close
#         new index
#     else:
#         open index
#         其实可以看whoosh的源码，refresh，如果判断索引文件有修改，直接refresh啊！！！查查refresh的运行的前后逻辑，是不是检查文件修改时间？
#
#     用self._ix.latest_generation() == self.reader....这个语句去判断索引文件是否修改？
#
#     sge.searchForGoods(arg_dict=arg_dict)
#
#     result=sge.result
#     print(result)
#
#
#
# def search_query_revise(arg_dict):
#
#     #空请求
#     if len(arg_dict['query'])==0:
#         return arg_dict['query']
#
#     qr=QueryRevise(arg_dict)
#     qr.seg_and_change()#精准分词
#     if qr.qBool:
#         qr.query_pinyin_revise()#拼音修正
#         # print(qr.query_pinyin)
#     query=qr.query_p_r
#     # 先分词，然后判断是否是pinyin字母，如果是拼音字母，则直接映射到电商词库字典的汉字
#     # 如果是汉字，先映射到拼音，再映射到电商词库字典的汉字
#     query_revise={"data":query,"resultCode":"0","msg":"成功"}
#     page_json=json.dumps(query_revise, ensure_ascii=False)##False解决中文乱码
#
#     return page_json
#
#
#
#
# if __name__=='__main__':
#     print('aaaa')
#     shopCode='-1'##或者为空也行shopCode=""
#     areaCode='-1'
#
#     sortMethod='null'##（3为按销量排，1为价格由低到高，2为价格由高到低）
#     querys=['蔬菜','蔬菜','蔬菜','蔬菜','蔬菜']
#     for q in querys:
#         arg_dict={}
#         arg_dict['query']=q
#         arg_dict['page']=1
#         arg_dict['rows']=50
#         arg_dict['areaCode']=areaCode
#         arg_dict['shopCode']=shopCode
#         arg_dict['sortMethod']=sortMethod
#         t0=time.time()
#         page_json=search_main(arg_dict)
#         t1=time.time()
#         time_used = t1 - t0
#         print("Time consumed(s): ", time_used)
#
#
#
#
#
#
#
#
#
#
#
#