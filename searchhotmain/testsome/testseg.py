# 导入一次jieba
import os
import jieba
# from jieba.analyse import ChineseAnalyzer
import jieba.analyse
#
# for query in ["白啤", "白酒", "白酒白啤", "名酒", "好酒"]:
#     seg_list = list(jieba.cut_for_search(query))
#     words = jieba.lcut_for_search(query, HMM=False)
#
#     print(seg_list, words, sep="\n")

# from jieba import analyse
pathDir=os.path.dirname(__file__)
configFilePath = os.path.join(pathDir, 'searchmatch', 'utils','words_brand_goods_all.txt')
jieba.load_userdict(configFilePath)
for query in ["白啤", "白酒", "白酒白啤", "名酒", "好酒"]:
    seg_list = list(jieba.cut_for_search(query))
    words = jieba.lcut_for_search(query, HMM=False)

    print(seg_list, words, sep="\n")


import jieba
# from jieba import analyse
pathDir=os.path.dirname(__file__)
configFilePath = os.path.join(pathDir, 'searchmatch', 'utils','words_brand_goods_all.txt')
jieba.load_userdict(configFilePath)

for query in ["白啤", "白酒", "白酒白啤", "名酒", "好酒"]:
    seg_list = list(jieba.cut_for_search(query))
    words = jieba.lcut_for_search(query, HMM=False)

    print(seg_list, words, sep="\n")

# from searchmatch.search_and_revise import SearchGoodsEgine, QueryRevise
# from searchmatch.index_update_cre import iu##两次load自定义词典之后，“白酒”没有分成“白”、“酒”

