# encoding=utf-8
from __future__ import unicode_literals
from whoosh.analysis import RegexAnalyzer, LowercaseFilter, StopFilter, StemFilter
from whoosh.analysis import Tokenizer, Token
from whoosh.lang.porter import stem

from whoosh.compat import text_type
from whoosh.compat import xrange

from whoosh.analysis import NgramTokenizer, NgramWordAnalyzer

from itertools import chain
import jieba
import re
import os

pathDir=os.path.dirname(__file__)
configFilePath = os.path.join(pathDir, 'utils', 'words_brand_goods_all.txt')
jieba.load_userdict(configFilePath)

# STOP_WORDS = frozenset(('a', 'an', '的'))
STOP_WORDS = frozenset(('这个', '的'))

accepted_chars = re.compile(r"[\u4E00-\u9FD5]+")


class ChineseTokenizer(Tokenizer):

    def __call__(self, text, **kargs):
        # words = jieba.tokenize(text, mode="search")
        words = jieba.tokenize(text, mode="search", HMM=False)  # 我改了呵呵，与search的时候分词方法应该一致
        token = Token()
        for (w, start_pos, stop_pos) in words:
            if not accepted_chars.match(w) and len(w) <= 1:
                continue
            token.original = token.text = w
            token.pos = start_pos
            token.startchar = start_pos
            token.endchar = stop_pos
            yield token


# 也可以继承，重写方法？
class ChineseTokenizer_Merge_Byme(Tokenizer):

    @staticmethod
    def chain_byme(a, b):
        for i in a:
            yield i

        for i in b:
            yield i

    @staticmethod
    def prt_iter(a):
        print(type(a))
        ct=0
        for i in a:
            print(i)
            ct+=1
        print("length:", ct)

    def __call__(self, text, **kargs):
        tk_chinese_gener=self.tk_chinese(text, **kargs)
        # self.prt_iter(tk_chinese_gener)
        # tk_ngram=NgramTokenizer(minsize=1, maxsize=4)##不过滤空格
        # tk_ngram=NgramWordAnalyzer(minsize=1, maxsize=4)##过滤空格
        tk_ngram = NgramWordAnalyzer(minsize=1, maxsize=2)##过滤空格
        # print(type(tk_ngram))
        tk_ngram_gener=tk_ngram(text, **kargs)
        # self.prt_iter(tk_ngram_gener)

        tk_gener= self.chain_byme(tk_chinese_gener, tk_ngram_gener)
        # self.prt_iter(tk_gener)
        # return self.tk_chinese(text, **kargs)
        # return tk_chinese_gener
        # return tk_ngram_gener
        return tk_gener


    def tk_chinese(self, text, **kargs):
        # words = jieba.tokenize(text, mode="search")
        words = jieba.tokenize(text, mode="search", HMM=False)  # 我改了呵呵，与search的时候分词方法应该一致
        token = Token()
        for (w, start_pos, stop_pos) in words:
            if not accepted_chars.match(w) and len(w) <= 1:
                continue
            token.original = token.text = w
            token.pos = start_pos
            token.startchar = start_pos
            token.endchar = stop_pos
            yield token

# class NgramTokenizer(Tokenizer):
#     """Splits input text into N-grams instead of words.
# 
#     >>> ngt = NgramTokenizer(4)
#     >>> [token.text for token in ngt("hi there")]
#     ["hi t", "i th", " the", "ther", "here"]
# 
#     Note that this tokenizer does NOT use a regular expression to extract
#     words, so the grams emitted by it will contain whitespace, punctuation,
#     etc. You may want to massage the input or add a custom filter to this
#     tokenizer's output.
# 
#     Alternatively, if you only want sub-word grams without whitespace, you
#     could combine a RegexTokenizer with NgramFilter instead.
#     """
# 
#     __inittypes__ = dict(minsize=int, maxsize=int)
# 
#     def __init__(self, minsize, maxsize=None):
#         """
#         :param minsize: The minimum size of the N-grams.
#         :param maxsize: The maximum size of the N-grams. If you omit
#             this parameter, maxsize == minsize.
#         """
# 
#         self.min = minsize
#         self.max = maxsize or minsize
# 
#     def __eq__(self, other):
#         if self.__class__ is other.__class__:
#             if self.min == other.min and self.max == other.max:
#                 return True
#         return False
# 
#     def __call__(self, value, positions=False, chars=False, keeporiginal=False,
#                  removestops=True, start_pos=0, start_char=0, mode='',
#                  **kwargs):
#         assert isinstance(value, text_type), "%r is not unicode" % value
# 
#         inlen = len(value)
#         t = Token(positions, chars, removestops=removestops, mode=mode)
#         pos = start_pos
# 
#         if mode == "query":
#             size = min(self.max, inlen)
#             for start in xrange(0, inlen - size + 1):
#                 end = start + size
#                 if end > inlen:
#                     continue
#                 t.text = value[start:end]
#                 if keeporiginal:
#                     t.original = t.text
#                 t.stopped = False
#                 if positions:
#                     t.pos = pos
#                 if chars:
#                     t.startchar = start_char + start
#                     t.endchar = start_char + end
#                 yield t
#                 pos += 1
#         else:
#             for start in xrange(0, inlen - self.min + 1):
#                 for size in xrange(self.min, self.max + 1):
#                     end = start + size
#                     if end > inlen:
#                         continue
#                     t.text = value[start:end]
#                     if keeporiginal:
#                         t.original = t.text
#                     t.stopped = False
#                     if positions:
#                         t.pos = pos
#                     if chars:
#                         t.startchar = start_char + start
#                         t.endchar = start_char + end
# 
#                     yield t
#                 pos += 1
# 

def ChineseAnalyzer(stoplist=STOP_WORDS, minsize=1, stemfn=stem, cachesize=50000):
    return (ChineseTokenizer() | LowercaseFilter() |
            StopFilter(stoplist=stoplist, minsize=minsize) |
            StemFilter(stemfn=stemfn, ignore=None, cachesize=cachesize))


def ChineseAnalyzerMerge(stoplist=STOP_WORDS, minsize=1, stemfn=stem, cachesize=50000):
    return (ChineseTokenizer_Merge_Byme() | LowercaseFilter() |
            StopFilter(stoplist=stoplist, minsize=minsize) |
            StemFilter(stemfn=stemfn, ignore=None, cachesize=cachesize))

# def ChineseAnalyzer(stoplist=STOP_WORDS, minsize=1, stemfn=stem, cachesize=50000):
#     return (ChineseTokenizer() | NgramTokenizer(minsize=1, maxsize=4) | LowercaseFilter() |
#             StopFilter(stoplist=stoplist, minsize=minsize) |
#             StemFilter(stemfn=stemfn, ignore=None, cachesize=cachesize))

# whoosh.analysis.acore.CompositionError: Only one tokenizer allowed at the start of the analyzer: [ChineseTokenizer(), LowercaseFilter(), StopFilter(stops=frozenset({'an', 'a', '的'}), min=1, max=None, renumber=True), StemFilter(stemfn=<function stem at 0x000002B73DE9E6A8>, lang=None, ignore=frozenset(), cachesize=50000, _stem=<function stem at 0x000002B744BF0488>), NgramTokenizer(min=1, max=4)]

# print(type(ChineseTokenizer()), type(NgramTokenizer(minsize=1, maxsize=4)))




if __name__=="__main__":

    for query in ["白啤", "白酒", "白酒白啤", "琪贝斯", "加强型", "自嗨锅", "a我是一个兵3"]:
        seg_list = list(jieba.cut_for_search(query))
        words = jieba.lcut_for_search(query, HMM=False)
        print(seg_list, words, sep="\n")

    import jieba.analyse
    a=jieba.analyse.extract_tags("我是一个兵3")
    print('hehe', a)
