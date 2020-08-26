# 字符串与list 和 dict的转换 ，两种方法
import json
a_list=[{"word": "梦田", "hot": False}, {"word": "不", "hot": True}]
a_str=str(a_list)
a_str2=json.dumps(a_list, ensure_ascii=False)
print(a_str, a_str2, sep="\n")


a_list=eval(a_str)
a_list2=json.loads(a_str2)
print(a_list, a_list2, sep="\n")


