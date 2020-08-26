from searches.esclient_byme import crud_es_sensitive, index_search_sense
query_ori = "我们在习近平总书记的领导下，批判周永康的罪行，习近平领导我们"

sensitive_data_list = [{"sensitive_word": "周永康", "autoid": 6079}]
res=crud_es_sensitive(sensitive_data_list, op_type="delete")
print(res)
query_res = index_search_sense(query_ori)
print(query_res)




a={"a": "1", "b": "2"}

print("a=%(a)s: b=%(b)s" % a)

def hehe():
    success="ss"
    failed='ff'
    stats_only=False
    errors=2

    return success, failed if stats_only else errors

a=hehe()
print(a)