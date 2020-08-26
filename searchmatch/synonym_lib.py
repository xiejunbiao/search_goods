# 同义词库


def list_set(i_list):
    i_set=set()
    for i in i_list:
        if len(i)>0:
            i_set.add(i)
    return i_set

# 双向同义词库
synonym_bi_list=['洗发水 洗发液 洗发精 洗发露 洗头膏 洗头水', '冰淇淋 冰激凌 甜筒', '雪糕 冰糕 冰棍']
synonym_bi_dict={}
for i in synonym_bi_list:
    i_list=i.split(" ")
    i_set=list_set(i_list)
    for key in i_set:
        synonym_bi_dict[key]=i_set
# 单向
# synonym_ui_dict={"冰激凌": {"冰棍", "冰糕", "雪糕"}, "冰淇淋": {"冰棍", "冰糕", "雪糕"}}
# 这是一个bug！！！把冰激凌转成了{"冰棍", "冰糕", "雪糕"}、、、草
synonym_ui_dict={"冰激凌": {"冰激凌", "冰淇淋", "冰棍", "冰糕", "雪糕"}, "冰淇淋": {"冰激凌", "冰淇淋", "冰棍", "冰糕", "雪糕"}}

synonym_dict={}
synonym_dict.update(synonym_bi_dict)
synonym_dict.update(synonym_ui_dict)
# print(synonym_dict)
if __name__=="__main__":
    print(synonym_dict)