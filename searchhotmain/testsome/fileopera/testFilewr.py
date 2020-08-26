# -*- coding: utf-8 -*-
"""
Created on Fri May 15 16:35:25 2020

@author: lijiangman
"""

"""
测试同时读文件
"""
import pickle

schema_dict={1:1, 2:2}
path_schema_dict=r'./path_schema_dict.pkl'
with open(path_schema_dict, 'wb') as f:
    pickle.dump(schema_dict, f)

# pickle.load(open(self.path_schema_dict, 'rb'))    

with open(path_schema_dict, 'rb') as f:
    schema_dict=pickle.load(f)    

