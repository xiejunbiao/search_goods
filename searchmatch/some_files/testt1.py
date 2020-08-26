# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:59:34 2019

@author: lijiangman
"""

# 也可以用gensim，鹏鹏

#with open('sgns.target.word-word.dynwin5.thr10.neg5.dim300.iter5','r') as f:
#    print(f.readlines)

path='sgns.target.word-word.dynwin5.thr10.neg5.dim300.iter5'
#path='sgns.wiki.bigram'

with open(path,'r',encoding='UTF-8') as lines:
    stw_list=lines.readlines()


#content = open(path, 'rb').read().decode('utf-8')
#good_words=[]
#for line in content.splitlines():
#    good_words.add(line)
