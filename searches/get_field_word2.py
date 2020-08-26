"""
1、2020-04-28：根据页面意图权重进行筛选页面意图
    优化将领域词打分策略嵌入进去增加内部方法
    _get_word_max_score和_get_max_from_dict
2、
"""
__all__ = ['get_field_word']
import sys
import os
from collections import Counter
# import ahocorasick
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(os.path.split(rootPath)[0])
sys.path.append(rootPath)
from voiceAssistant.analysisAlgorithm.ahoCorasick import Ahocorasick
from voiceAssistant.getWord import get_score_page_intent


def _get_key(dict_, value):
    return [k for k, v in dict_.items() if v == value]


def _get_max_from_dict(dict):
    """
    获取字典中value最大值所对应的键列表（可能存在多个）
    :param dict:
    :return:
    """
    # 获取字典中value最大值所对应的键（当存在两个时返回字典中靠前的一个）
    field_key = max(dict, key=dict.get)
    result = dict[field_key]
    # _get_key(dict, result) 获取字典中value=result的所对应的键
    if len(_get_key(dict, result)) != 1:
        return dict.keys()
    else:
        return [field_key]


def _get_word_max_score(word_list):
    """
    根据统计页面意图的分值获取最大词列表
    :param word_list:
    :return:
    """
    count_result = Counter(word_list)
    score_dir = {}

    # 使用字典存放每个领域词的得分
    for each_word in count_result.keys():
        score_dir[each_word] = count_result[each_word] * get_score_page_intent(each_word)
    print(score_dir)
    result_list = _get_max_from_dict(score_dir)
    return result_list


def get_field_word(words_dir, InputTxt):
    """

    :param words_dir:
    :param InputTxt:
    :return: 最大匹配词列表
    """
    result = []
    # 实例化Ahocorasick算法
    ah = Ahocorasick()
    for word in words_dir.keys():
        ah.addWord(word)
    ah.make()

    # 通过ah算法得到领域词索引列表
    results = ah.search(InputTxt)

    # 将领域词转换为页面意图词列表
    word_list_all = []
    if len(results) == 0:
        return result
    else:
        for site in results:
            w = InputTxt[site[0]:site[1]+1]
            word_list_all = word_list_all + words_dir[w]

    # 按照页面意图打分获取到的页面意图列表
    result_word_list = _get_word_max_score(word_list_all)
    return result_word_list


"""
以下使用python中的ahocorasick库（linux中需要安装）
其中没有考虑结果为空的情况
"""
# def search_field_word(words_dir, inputtxt):
#     actree = ahocorasick.Automaton()
#     for index, word in enumerate(words_dir.keys()):
#         actree.add_word(word, (index, word))
#     actree.make_automaton()
#     target_wds = []
#     word_list_all = []
#     contxt = []
#     # 此处应该没有考虑结果为空的情况
#     for i in actree.iter(inputtxt):
#         wd = i[1][1]  # i的形式为(index,(index,word))
#         contxt.append(inputtxt[i[0]+1:])
#         target_wds.append(wd)
#         word_list_all = word_list_all + words_dir[wd]
#     if len(word_list_all) == 1:
#         return word_list_all
#     else:
#         result_word_list = _get_word_max_score(word_list_all)
#         return result_word_list
