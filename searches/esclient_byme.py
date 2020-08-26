from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import json, traceback
from searches.data_analyse import generate_actions, generate_actions_sense, generate_actions_sense_test
from searchhotmain.inition_config import config_result  # 数据库配置

# from searches.analyzer_byme import jieba, stopwords
# from searches.pyahocorasick import Trie
# import re

punc = '~`!#$%^&*()_+-=|\';":/.,?><~·！\\\@#￥%……&*（）——+-=“：’；、。，？》《{} '
punc_list=[i for i in punc]
# es=Elasticsearch([{"host": "10.18.226.38", "port": 9200}])
# ["host1", "host2"], maxsize=25
# ["192.168.4.7", "192.168.4.14", "192.168.4.15"]
# ["10.18.226.84", "10.18.226.86", "10.18.226.79"]

# 连接host='10.18.226.86'，host='10.18.226.84'偶发性超时！！！
# elasticsearch.exceptions.ConnectionTimeout: ConnectionTimeout caused by - ReadTimeoutError(HTTPConnectionPool(host='10.18.226.84', port=9200): Read timed out. (read timeout=10))

# es.indices.delete(index=index_bulk_sense, ignore=[400, 404]) 也报相同的错
# 可能的原因：数据较大，网络不好。解决方案：增加超时时间，增加异常重试。

# 将全局超时设置为timeout = 1, 竟然复现了报错！！！！哈哈哈哈
# 设置了超时+重试之后，效果很明显，感谢stackoverflow.com/questions/25908484/how-to-fix-read-timed-out-in-elasticsearch
# 多线程 By default we allow urllib3 to open up to 10 connections to each node, if your application calls for more parallelism, use the maxsize parameter to raise the limit:
# es=Elasticsearch(["10.18.226.84", "10.18.226.86", "10.18.226.79"],
es_host=[config_result['es_host1'], config_result['es_host2'], config_result['es_host3']]
es=Elasticsearch(hosts=es_host,
                 maxsize=25,
                 port=9200,
                 sniff_on_start=True,  # 报错raise ConnectionError("N/A", str(e), e) 连接超时
                 sniff_on_connection_fail=True,
                 sniffer_timeout=600,
                 # sniff_timeout=60,
                 timeout=6000,
                 max_retries=100,
                 retry_on_timeout=True,
                 http_auth=("elastic", "1qaz2WSX3edc"),
                 scheme="http"
                 # scheme="https"#OpenSSL.SSL.Error: [('SSL routines', 'ssl3_get_record', 'wrong version number')]
                 )

# es=Elasticsearch(["10.18.226.84", "10.18.226.86", "10.18.226.79"], maxsize=25,
#                  port=9200,
#                  sniff_on_start=True,  # 报错raise ConnectionError("N/A", str(e), e) 连接超时
#                  sniff_on_connection_fail=True,
#                  sniffer_timeout=60,
#                  # sniff_timeout=60,
#                  # timeout=60,
#                  http_auth=("elastic", "1qaz2WSX3edc"),
#                  scheme="http"
#                  # scheme="https"#OpenSSL.SSL.Error: [('SSL routines', 'ssl3_get_record', 'wrong version number')]
#                  )

# es.nodes.info(node_id='_all', metric='clear')#检查是否能连接sniff

index_bulk="test-index-bulk-2"
index_bulk_cook="index_cook_6"
# index_bulk_sense="test_env_index_sense_word_17"  # 开发和测试环境共用一个--改为可配置的！类似数据库？？
# index_bulk_sense="cb_es_sensitive_words"  # 开发和测试环境共用一个--改为可配置的！类似数据库？？
index_bulk_sense = config_result["cb_es_sensitive_words"]

# my_analyzer = analyzer('my_analyzer',
#     tokenizer=tokenizer('trigram', 'nGram', min_gram=3, max_gram=3),
#     filter=['lowercase']
# )


def create_index_sense():
    """Create an index in Elasticsearch from scrash"""
    """
    ngram分词
    """
    # es.indices.delete(index=index_bulk_sense, ignore=[400, 404])#报错，    raise ConnectionError("N/A", str(e), e)

    # es_del = Elasticsearch(["10.18.226.84", "10.18.226.86", "10.18.226.79"], maxsize=25, port=9200, http_auth=("elastic", "1qaz2WSX3edc"), scheme="http")
    # es_del.indices.delete(index=index_bulk_sense, ignore=[400, 404])
    es.indices.delete(index=index_bulk_sense, ignore=[400, 404])

    # PUT
    # standard_tokenizer_index
    # {
    #     "settings": {
    #         "analysis": {
    #             "analyzer": {
    #                 "my_analyzer": {
    #                     "tokenizer": "my_tokenizer"
    #                 }
    #             },
    #             "tokenizer": {
    #                 "my_tokenizer": {
    #                     "type": "standard",
    #                     "max_token_length": 5
    #                 }
    #             }
    #         }
    #     }
    # }
    #
    # POST
    # standard_tokenizer_index / _analyze
    # {
    #     "analyzer": "my_analyzer",
    #     "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
    # }
    es.indices.create(
        index=index_bulk_sense,
        body={
            "settings": {
                "index.max_ngram_diff": 15,
                "analysis": {
                    "analyzer": {
                        "my_analyzer": {
                            "tokenizer": "my_tokenizer"
                        }
                    },
                    "tokenizer": {
                        "my_tokenizer": {
                            "type": "ngram",
                            # "max_ngram_diff": 16,
                            "min_gram": 1,
                            "max_gram": 10,  # 加入这个参数的时候，大于等于3报错，找不到我的分词器
                            "token_chars": [
                                "letter",
                                "digit"
                            ]
                            #                         "type": "standard",
                            #                         "max_token_length": 5
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "type_s": {
                        "type": "text",
                        # "analyzer": "standard",
                        "analyzer": "ik_max_word",
                    },
                    "sensitive_word": {
                        "type": "text",
                        "analyzer": "keyword",  # 之前写成“key_word”，结果按照单字分词了。应该是 "analyzer": "keyword"},
                        "search_analyzer": "my_analyzer"}
                }
            }
        },
        ignore=400,
        request_timeout=60
    )
    # es.indices.create(
    #     index=index_bulk_sense,
    #     body={
    #         "settings": {
    #             # "number_of_shards": 1,
    #             "analysis": {
    #                "analyzer": {
    #                     # "my_analyzer": {
    #                     #     "type": "custom",
    #                     #     "tokenizer": "standard"
    #                     #     # "tokenizer": "my_tokenizer"
    #                     # },
    #                    # "my_analyzer": {
    #                    #     # "type": "ngram",
    #                    #     "tokenizer": "ngram"
    #                    #     # "tokenizer": "my_tokenizer"
    #                    # },
    #                    "my_analyzer": {
    #                        # "type": "ngram",
    #                        # "tokenizer": "ngram"
    #                        "tokenizer": "my_tokenizer"
    #                    },
    #
    #                    # "tokenizer": {
    #                     #    "my_tokenizer": {
    #                     #        "type": "ngram",
    #                     #        "min_gram": 1,
    #                     #        "max_gram": 12,
    #                     #        "token_chars": ["letter", "digit"]
    #                     #    }
    #                     # }
    #                },
    #                "tokenizer": {
    #                     "my_tokenizer": {
    #                         "type": "ngram",
    #                         "min_gram": 1,
    #                         "max_gram": 12,
    #                         "token_chars": ["letter", "digit"]
    #                     }
    #                 }
    #
    #             },
    #         },
    #         "mappings": {
    #             "properties": {
    #                 "type_s": {
    #                     "type": "text",
    #                     # "analyzer": "standard",
    #                     "analyzer": "ik_max_word",
    #                 },
    #                 "sensitive_word": {
    #                     "type": "text",
    #                     "analyzer": "keyword",# 之前写成“key_word”，结果按照单字分词了。应该是 "analyzer": "keyword"},
    #                     "search_analyzer": "my_analyzer"}
    #                 }
    #             }
    #         },
    #     ignore=400,
    #     request_timeout=60
    # )


def create_index_sense_ik():
    """Create an index in Elasticsearch from scrash"""
    # es.indices.delete(index=index_bulk_sense, ignore=[400, 404])#报错，    raise ConnectionError("N/A", str(e), e)

    # es_del = Elasticsearch(["10.18.226.84", "10.18.226.86", "10.18.226.79"], maxsize=25, port=9200, http_auth=("elastic", "1qaz2WSX3edc"), scheme="http")
    # es_del.indices.delete(index=index_bulk_sense, ignore=[400, 404])
    es.indices.delete(index=index_bulk_sense, ignore=[400, 404])

    es.indices.create(
        index=index_bulk_sense,
        body={
            "settings": {"number_of_shards": 1},
            "mappings": {
                "properties": {
                    "type_s": {
                        "type": "text",
                        # "analyzer": "standard",
                        "analyzer": "ik_max_word",
                        # "search_analyzer": "ik_smart"
                    },
                    "sensitive_word": {
                        "type": "text",
                        "analyzer": "keyword"}#之前写成“key_word”，结果按照单字分词了。而且要注意，既然是敏感词，就是一个整体，不用分词呢
                        # "search_analyzer": "my_analyzer"}
                    }
                }
            },
        ignore=400,
        request_timeout=60
    )


def index_sense():

    helpers.bulk(client=es, index=index_bulk_sense, actions=generate_actions_sense())#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？

    # helpers.bulk(client=es, index=index_bulk_sense, actions=generate_actions_sense_test())#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？
    # elasticsearch.exceptions.ConnectionTimeout: ConnectionTimeout caused
    # by - ReadTimeoutError(HTTPConnectionPool(host='10.18.226.86', port=9200): Read
    # timed out.(read timeout = 10))

    es.indices.refresh(index=index_bulk_sense)


def res_gener(haystack, needles):

    # res_gen = [i for i in haystack]
    # t = Trie()
    # for w in needles:
    #     t.add_word(w, w)
    #
    # t.make_automaton()
    # # for res in t.items():
    # #     print(res)
    #
    # for res in t.iter(haystack):
    #     # pos, matches = res
    #     end_index, matches = res
    #     for fragment in matches:
    #         start_index = (end_index - len(fragment) + 1)
    #         res_gen[start_index:end_index]="*"

    for i in needles:
        haystack=haystack.replace(i, "*"*len(i))

    return haystack


def index_search_sense(query_ori):

    # 先删除标点符号
    # query_ori="我们在习#近,平总书记的领导下，批判周\永 、\康的罪行，习\\近 平领导我们"
    # query=re.sub(r"[%s]+" % punc, "", query_ori)
    # 输出符号的位置，还原字符串
    query_list = []
    index_str = []
    index_q = -1
    for i in query_ori:
        index_q += 1
        if i not in punc_list:
            query_list.append(i)
        else:
            index_str.append((i, index_q))
    query="".join(query_list)

    # 停用词 “的”
    # words = jieba.lcut_for_search(query_ori, HMM=False)
    # words = [w for w in words if w not in stopwords]
    # query = "".join(words)
    # print(query)
    # 如何查看 index_bulk_sense的索引？es.termvectors()单条文档的索引

    # data_search(index_bulk_cook, query)
    # body={"query": {"match_all": {}}}
    body={
        "query": {
            "multi_match": {
                "operator": "or",
                "query": query,
                # "analyzer": "keyword",
                # "analyzer": "ik_max_word",
                "analyzer": "my_analyzer",
                # "search_analyzer": "my_analyzer",
                "fields": ["sensitive_word"]
                }
            },
        "highlight": {
            "fields": {
                "sensitive_word": {}
            }
        },
        "size": 1000,
        "from": 0,
    }
    # res=es.search(index=index, body=body, size=10)
    res=es.search(index=index_bulk_sense, body=body)
    # body_term={
    #     "fields": ["sensitive_word"]
    # }
    # res_term=es.termvectors(index=index_bulk_sense, id="Viz113MB0UhROFCXOTHP", body=body_term)
    # print(res_term)

    # print("Got %d Hits:" % res['hits']['total']['value'])
    # for hit in res['hits']['hits']:
        # print("%(author)s: %(text)s" % hit["_source"]["doca"])
        # print(hit['_id'], hit['_source'], sep=' ')
        # print(hit)
    res_sense_tep=[hit['_source']['sensitive_word'] for hit in res['hits']['hits']]
    res_sense=sorted(res_sense_tep, key=lambda i: len(i), reverse=True)#傻逼艹、傻逼同时命中的情况下，如何返回query？取并集？按照长度从高到低replace
    # print(res_sense)
    if len(res_sense)>0:
        # res_gen=res_gener(haystack=query_ori, needles=res_sense)\
        res_gen = res_gener(haystack=query, needles=res_sense)
        res_gen_ori=list(res_gen)
        if len(index_str) > 0:
            # print(index_str)
            for (i, index_q) in index_str:
                res_gen_ori.insert(index_q, i)

        query_ori="".join(res_gen_ori)
        queryResult="1"
    else:
        queryResult = "0"

    data_dict = {
        "queryResult": queryResult,
        "queryRes": query_ori
    }
    query_res = {
        "resultCode": "0",
        "msg": "操作成功",
        "data": data_dict
        #               'testSpu':spu_code_search##测试分页的正确性
    }
    query_res = json.dumps(query_res, ensure_ascii=False)
    # print(query_res)
    return query_res


def create_index_cook():
    """Create an index in Elasticsearch from scrash"""
    es.indices.delete(index=index_bulk_cook, ignore=[400, 404])
    es.indices.create(
        index=index_bulk_cook,
        body={
            "settings": {"number_of_shards": 1},
            "mappings": {
                "properties": {
                    "name": {
                        "type": "text",
                        # "analyzer": "standard",
                        "analyzer": "ik_max_word",
                        # "search_analyzer": "ik_smart"
                    },
                    "ingredients": {
                        "type": "text",
                        "analyzer": "ik_max_word"},
                    "methods": {
                        "type": "text",
                        # "analyzer": "keyword",
                        "analyzer": "ik_max_word",
                        # "search_analyzer": "ik_smart"
                    }
                }
            }
        },
        ignore=400
    )


def index_cook():

    helpers.bulk(client=es, index=index_bulk_cook, actions=generate_actions())#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？
    es.indices.refresh(index=index_bulk_cook)


def index_search_cook():
    query="西红柿炒鸡蛋姜"
    query="切去头尾，刮去皮"
    # query="小米椒"
    query="鸡蛋"
    query_ori = "好吃的葱头"
    query = "好吃的葱头"
    # query = "好吃葱头。，" #标点被忽略了
    # 停用词 “的”
    # words = jieba.lcut_for_search(query_ori, HMM=False)
    # words = [w for w in words if w not in stopwords]
    # query = " ".join(words)
    body={
        "query": {
            "multi_match": {
                "operator": "or",
                "query": query,
                # "analyzer": "keyword",
                "analyzer": "ik_max_word",
                "fields": ["name^2",  # multi field boost，对score乘以一个系数
                           "ingredients",
                           "methods"]
                }
            },
        "size": 10,
        "from": 0,
    }
    # res=es.search(index=index, body=body, size=10)
    res=es.search(index=index_bulk_cook, body=body)

    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        # print("%(author)s: %(text)s" % hit["_source"]["doca"])
        print(hit['_id'], hit['_source'], sep=' ')
        print(hit)


def data_init():
    # 全量初始化
    es.indices.delete(index=index_bulk)
    es.indices.create(index=index_bulk, ignore=400)
    # es.indices.create(index=index_bulk)

    """ 批量插入数据 
    注意考虑1000条一次？？？？
    ????
    """
    # '_op_type':'index',#操作 index update create delete
    action = [
        {
            "_op_type": "index",
            "_index": index_bulk,
            "_id": str(i),
            "_source": {
                "doca": {
                    # "_id": str(i),
                    'author': 'kimchy',
                    'text': 'Elasticsearch: cool. bonsai cool 2.' + str(i),
                },
                'author': 'kimchy',
                'text': 'Elasticsearch: cool. bonsai cool 2.' + str(i)
            }
        } for i in range(10)]

    # action = [
    #     {
    #         "_op_type": "index",
    #         "_index": index_bulk,
    #         "_id": str(i),
    #         # 如果不加"_source"，默认是_source里面的文档字典
    #         "doc": {
    #             "_id": str(i),
    #             'author': 'kimchy',
    #             'text': 'Elasticsearch: cool. bonsai cool 2.' + str(i),
    #         }
    #     } for i in range(10)]

    helpers.bulk(client=es, actions=action)  # id自动生成
    es.indices.refresh(index=index_bulk)
    data_search(es)


def sensitive_data_search(index, query):
    # body={"query": {"match_all": {}}}
    body={
        "query": {
            "multi_match": {
                "operator": "or",
                "query": query,
                # "analyzer": "keyword",
                "analyzer": "ik_max_word",
                "fields": ["sensitive_word"]
                }
            },
        "highlight": {
            "fields": {
                "sensitive_word": {}
            }
        },
        "size": 1000,
        "from": 0,
    }
    # res=es.search(index=index, body=body, size=10)
    res=es.search(index=index, body=body)

    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        # print("%(author)s: %(text)s" % hit["_source"])
        # print("%(author)s: %(text)s" % hit["_source"]["doca"])
        print(hit)


def data_search(index, query):
    # body={"query": {"match_all": {}}}
    body={
        "query": {
            "multi_match": {
                "operator": "or",
                "query": query,
                "fields": ["name", "ingredients", "methods"]
                }
            },
        "size": 10,
        "from": 0,
    }
    # res=es.search(index=index, body=body, size=10)
    res=es.search(index=index, body=body)

    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        # print("%(author)s: %(text)s" % hit["_source"])
        # print("%(author)s: %(text)s" % hit["_source"]["doca"])
        print(hit)


def data_update_bulk():
    # 增量更新
    query={'query': {'match': {'author': 'kimchy'}},
           # ""doc"" is essentially Elasticsearch's ""_source"" field
           '_source': {
               'author': 'kimchy',
               'text': 'Elasticsearch: cool. bonsai cool 2.',
           }
        }
    # es.update(index=index_bulk, body=query)  # 更新
    body={
        "query": {
            "match": {
                "author": "kimchy"
            }
        }
    }

    actions={
        '_op_type': 'update',
        '_index': index_bulk,
        '_id': 3,
        '_source':
            {
                'author': 'ljm',
                'text': 'Elasticsearch is good.'
            }
    }

    def actions_gen():
        ids=['3']
        # 可先delete，再update
        for id in ids:
            yield {
            '_op_type': 'update',
            '_index': index_bulk,
            '_id': id,
            'doc':#doc相当于_source
                {
                 'doca': {'author': 'ljm_update'},# 更新嵌套的字典？
                 'question': 'ttt',
                 'author': "Ljm",
                 'text': 'Elasticsearch is good.'
                 },
            # 'doca':{'question': 'ttt', 'author': "Ljm", 'text': 'Elasticsearch is good.'}
            # 'author': 'ljm',
            # 'text': 'Elasticsearch is good.'
            }

    helpers.bulk(client=es, actions=actions_gen())#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？
    es.indices.refresh(index=index_bulk)
    data_search(es)


def data_update_query():
    # 修改已有的字段
    # 假如我们想对所有在北京的文档里的uid都加1，那么我么有通过如下的方法：
    #
    # POST
    # twitter / _update_by_query
    body={
        "query": {
            "match": {
                "author.keyword": "kimchy"
            }
        },
        "script": {
            "source": "ctx._source['text'] = 'test_by_ljm'",
            "params": {
                "one": 1
            }
        }
    }
    resp=es.update_by_query(index=index_bulk, body=body)  # 更新
    # print(resp)
    es.indices.refresh(index=index_bulk)
    data_search(es)


def test2():
    # es.index(index="my-index-test-000001", doc_type="test-type", id=42, body={"any": "data", "timestamp": datetime.now()})
    # es.index(index="my-index-test-000001", id=42, body={"any": "data_42", "timestamp": datetime.now()})
    # es.index(index="my-index-test-000001", id=0, body={"any": "data_0", "timestamp": datetime.now()})
    # es.index(index="my-index-test-000001", id=0, body={"any": "data_0 data_1", "timestamp": datetime.now()})
    # es.index(index="my-index-test-000001", id=1, body={"any": "data_1", "timestamp": datetime.now()})

    doc={
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool.',
        'timestamp': datetime.now()
    }
    # res=es.index(index="test-index", id=1, body=doc)
    # create or update的规则是什么？
    res=es.index(index="test-index", body=doc)  # id自动生成
    # 如何建立二级索引  多级索引名字？？？  索引文件的目录结构是怎么样的？ 看看38服务器？ 都是文件夹的形式？？
    # 如何设置索引的路径目录？？
    # 用索引名字就可以设置索引级别？
    # logstash-2015.08.20
    # sqyn-test
    # sqyn-product
    # sqyn-area-001
    # sqyn-area-002
    print(res, res['result'], sep='\n')
    res=es.get(index="test-index", id=1)
    print(res['_source'])

    es.indices.refresh(index="test-index")
    res=es.search(index="test-index", body={"query": {"match_all": {}}})

    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])


def test():
    # 批量写入json
    # index_bulk="test-index-bulk-2"
    doc1={
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool 1.',
        'timestamp': datetime.now()
    }

    doc2={
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool 2.',
        'timestamp': datetime.now()
    }
    # doc_list=[json.dumps(doc1, ensure_ascii=False), json.dumps(doc2, ensure_ascii=False)]
    # doc="\n".join(doc_list)

    """ 批量写入数据 """
    # '_op_type':'index',#操作 index update create delete
    action = [
        {
        "_op_type": "index",
        "_index": index_bulk,
        "_source": {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool 2.'+str(i),
        }
    } for i in range(10)]

    action = [
        {
        "_op_type": "create",
        # // 设置操作类型type就必须设置id，否则报错
        "_id":1,
        "_index": index_bulk,
        "_source": {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool 2.'+str(i),
        }
    } for i in range(10)]

    # 当_op_type是update的时候，action中必须包含'doc'关键词，用于表示更新的内容。而_op_type是index的时候，action中则必须包含'_source'关键词，表示插入的内容。而_op_type提供了默认值index，所以最后成功的那个操作已经变为了插入，而不是更新。
    action = [
        {
        "_op_type": "index",
        # // 设置操作类型type就必须设置id，否则报错
        "_id":i,
        "_index": index_bulk,
        "_source": {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool 2.'+str(i),
        }
    } for i in range(10)]

    helpers.bulk(client=es, actions=action)  # id自动生成

    es.indices.refresh(index=index_bulk)
    res=es.search(index=index_bulk, body={"query": {"match_all": {}}}, size=3)

    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        # print("%(author)s: %(text)s" % hit["_source"])
        print(hit)

    # get--Returns a document.
    document_data=es.get(index="my-index-test-000001", id=42, refresh=False)["_source"]
    print(type(document_data), document_data, sep="\n")
    document_data=es.get(index="my-index-test-000001", id=0, refresh=False)["_source"]
    print(type(document_data), document_data, sep="\n")

    # search--Returns results matching a query.---在es更新之后，有几秒的延迟才能被search所见
    search_results=es.search(index="my-index-test-000001", q='any: "data_0"')['hits']
    print(type(search_results), search_results, sep="\n")

    search_results=es.search(index="my-index-test-000001", q='any: "data_1"')['hits']
    print(type(search_results), search_results, sep="\n")

    search_results=es.search(index="my-index-test-000001")['hits']['hits']
    print(type(search_results), search_results, sep="\n")

    ids=[i['_id'] for i in search_results]
    sources=[i['_source'] for i in search_results]
    sources_any=[i['_source']['any'] for i in search_results]
    print(ids)
    print(sources)
    print(sources_any)

def sensitive_data_bulk_delete(sensitive_data_list):

    """---delete的时候，不用加doc字段"""
    """client 说明"""
    # def index(self, index, body, doc_type=None, id=None, params=None, headers=None):
    # Creates or updates    a    document in an    index.
    # ` < https: // www.elastic.co / guide / en / elasticsearch / reference / 7.8 / docs - index_.html > `_
    # 但是官网< https: // www.elastic.co / guide / en / elasticsearch / reference / 7.8 / docs - index_.html > 的说明更加精确
    # Adds a JSON document to the specified index and makes it searchable. If the document already exist, update the document
    # and increments its version. 这里会自动升级索引的版本。索引是唯一的？？？先删除再增加比较靠谱？delete create？

    def actions_gen():
        # ids=['3']
        # 可先delete，再update
        for sd in sensitive_data_list:
            doc_s= {
                # '_op_type': 'update',只能更新已有的id
                '_op_type': 'delete',  #操作 index update create delete----delete的时候，不用加doc字段
                '_index': index_bulk_sense,
                '_id': sd["autoid"],
            }
            yield doc_s
    # 有一个奇怪的现象，就是如果分别处理update和create 会出现版本冲突的报错！！！？？？？？？为什么
    # create的语法和update的不同！！！create要删除doc！！！？？？
    # delete 只用id 极简主义
    helpers.bulk(client=es, actions=actions_gen(), raise_on_error=False)#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？
    es.indices.refresh(index=index_bulk_sense)


def sensitive_data_bulk_create(sensitive_data_list):

    """---delete的时候，不用加doc字段"""
    """client 说明"""
    # def index(self, index, body, doc_type=None, id=None, params=None, headers=None):
    # Creates or updates    a    document in an    index.
    # ` < https: // www.elastic.co / guide / en / elasticsearch / reference / 7.8 / docs - index_.html > `_
    # 但是官网< https: // www.elastic.co / guide / en / elasticsearch / reference / 7.8 / docs - index_.html > 的说明更加精确
    # Adds a JSON document to the specified index and makes it searchable. If the document already exist, update the document
    # and increments its version. 这里会自动升级索引的版本。索引是唯一的？？？先删除再增加比较靠谱？delete create？

    def actions_gen():
        # ids=['3']
        # 可先delete，再update
        for sd in sensitive_data_list:
            doc_s= {
                # '_op_type': 'update',只能更新已有的id
                '_op_type': 'create',  #操作 index update create delete----delete的时候，不用加doc字段
                '_index': index_bulk_sense,
                '_id': sd["autoid"],
                'type_s': 'sense',
                'sensitive_word': sd["sensitive_word"],
                # 'ignore': 409
            }
            yield doc_s
    # 有一个奇怪的现象，就是如果分别处理update和create 会出现版本冲突的报错！！！？？？？？？为什么
    # create的语法和update的不同！！！create要删除doc！！！？？？
    # delete 只用id 极简主义

    # helpers.streaming_bulk(client=es, actions=actions_gen())
    # helpers.bulk(client=es, actions=actions_gen(), stats_only=True)
    helpers.bulk(client=es, actions=actions_gen(), raise_on_error=False)  # 看源代码真香
    # helpers.bulk(client=es, actions=actions_gen(), raise_on_error=True)  # 看源代码真香

    # helpers.bulk(client=es, actions=actions_gen(), raise_on_exception=False)#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？
    # helpers.bulk(client=es, actions=actions_gen(), raise_on_exception=True)#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？
    es.indices.refresh(index=index_bulk_sense)


def sensitive_data_bulk_update(sensitive_data_list):

    """---delete的时候，不用加doc字段"""
    """client 说明"""
    # def index(self, index, body, doc_type=None, id=None, params=None, headers=None):
    # Creates or updates    a    document in an    index.
    # ` < https: // www.elastic.co / guide / en / elasticsearch / reference / 7.8 / docs - index_.html > `_
    # 但是官网< https: // www.elastic.co / guide / en / elasticsearch / reference / 7.8 / docs - index_.html > 的说明更加精确
    # Adds a JSON document to the specified index and makes it searchable. If the document already exist, update the document
    # and increments its version. 这里会自动升级索引的版本。索引是唯一的？？？先删除再增加比较靠谱？delete create？

    def actions_gen():

        # {'_index': 'test_env_index_sense_word_14', '_type': '_doc', '_id': '6079', '_score': 6.915723,
        #  '_source': {'type_s': 'sense', 'sensitive_word': '周永康'}, 'highlight': {'sensitive_word': ['<em>周永康</em>']}}

        # 多了一个键：doc
        # {'_index': 'test_env_index_sense_word_14', '_type': '_doc', '_id': '60791111', '_version': 1, '_seq_no': 1516,
        #  '_primary_term': 1, 'found': True, '_source': {'doc': {'type_s': 'sense', 'sensitive_word': '周永康'}}}

        # ids=['3']
        # 可先delete，再update
        for sd in sensitive_data_list:
            doc_s = {
                '_op_type': 'update',  # 只能更新已有的id # '_op_type': op_type,  #操作 index update create delete----delete的时候，不用加doc字段
                '_index': index_bulk_sense,
                '_id': sd["autoid"],
                'doc': # doc相当于_source
                    {
                     'type_s': 'sense',
                     'sensitive_word': sd["sensitive_word"]
                     },
                # ignore: [404]
            }
            yield doc_s

    # 有一个奇怪的现象，就是如果分别处理update和create 会出现版本冲突的报错！！！？？？？？？为什么
    # create的语法和update的不同！！！create要删除doc！！！？？？
    # delete 只用id 极简主义
    helpers.bulk(client=es, actions=actions_gen(), raise_on_error=False)#如何根据query查询，或者根据某字段进行update更新？唯一主键当做条件？
    es.indices.refresh(index=index_bulk_sense)


def main():
    print("Creating an index...")
    # create_index_cook()
    # create_index_sense_ik()
    create_index_sense()
    print("Index cook documents...")
    # index_cook()
    index_sense()
    print("Searching cook documents...")
    # index_search_cook()
    # query="西红柿炒鸡蛋姜"
    # query_ori="切去头尾，刮去皮"
    # query_ori="习近平"
    # query_ori="习 近平"
    # query = "习,近平"
    # query = "习，近平"
    # query_ori="他妈的真难吃"
    query_ori="一群傻逼艹周永康，但是我们却热爱哦习近平"
    #减少ngram的长度，可以少于max_clause_count默认1024
    # elasticsearch.exceptions.RequestError: RequestError(400, 'search_phase_execution_exception',
    #                                                     'too_many_clauses: maxClauseCount is set to 1024')

    query_ori="""<p>傻逼<img src="http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82f8004ea9f343be9ca50df5eacd6410.jpg"></p><p>傻逼<img src="http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"""
    # elasticsearch.exceptions.RequestError: RequestError(400, 'search_phase_execution_exception',
    #                                                     'failed to create query: {\n  "multi_match" : {\n    "query" : "p傻逼imgsrchttp21914731388125hisensedownloadmerchantPCgoodsPicjpg82f8004ea9f343be9ca50df5eacd6410jpgpp傻逼imgsrchttp21914731388125hisensedownloadmerchantPCgoodsPicjpg82p",\n    "fields" : [\n      "sensitive_word^1.0"\n    ],\n    "type" : "best_fields",\n    "operator" : "OR",\n    "analyzer" : "my_analyzer",\n    "slop" : 0,\n    "prefix_length" : 0,\n    "max_expansions" : 50,\n    "zero_terms_query" : "NONE",\n    "auto_generate_synonyms_phrase_query" : true,\n    "fuzzy_transpositions" : true,\n    "boost" : 1.0\n  }\n}')

    # query_ori=r"""<p>傻逼<img src="http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82f8004ea9f343be9ca50df5eacd6410.jpg"></p><p>傻逼<img src="http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"""
    # 即使r不转义，也会随机出现两种error，一种是maxClauseCount，一种是failed to create query
    # query_ori="""<p>傻逼<img src="http://219.147.31.38:8125/hisense/download/merchantPC/goodsPic/jpg/82"></p>"""

    # query_ori="胜多负少的方式个三公分的地方让国人愤怒的第二代身份证asjalkjsalhj"
    # query_ori="小米椒"
    # query_ori="我们在习近平总书记的领导下，批判周永康的罪行，习近平领导我们"
    # query_ori = "我们在习近平总书记的领导下批判周永康的罪行习近平领导我们"
    # query_ori="我们在习近平总书记的领导下，批判周永康的罪行，习近平领导我们"
    # query_ori="我们在习#近,平总书记的领导下，批判周\永 、\康的罪行，习\\近 平领导我们"
    query_res=index_search_sense(query_ori)
    print(query_res)

    # sensitive_data_search(index_bulk_sense, "周永康")
    # document_data = es.get(index=index_bulk_sense, id=6079, refresh=False)
    # print(type(document_data), document_data, sep="\n")

    sensitive_data_list=[{"sensitive_word": "习仲勋测试", "autoid": 6081}, {"sensitive_word": "周永康测试", "autoid": 6079}]
    # sensitive_data_bulk_update(sensitive_data_list)
    crud_es_sensitive(sensitive_data_list, op_type="update")
    query_res = index_search_sense(query_ori)
    print(query_res)

    # sensitive_data_list=[{"sensitive_word": "习近平测试", "autoid": 6080}, {"sensitive_word": "周永康", "autoid": 60811111111111}]  找不到id的话 会报错，怎么办
    sensitive_data_list=[{"sensitive_word": "习近平测试", "autoid": 6080}, {"sensitive_word": "周永康测试", "autoid": 6079}]
    # sensitive_data_bulk_delete(sensitive_data_list)
    crud_es_sensitive(sensitive_data_list, op_type="delete")
    query_res = index_search_sense(query_ori)
    print(query_res)

    sensitive_data_list=[{"sensitive_word": "习仲勋啊", "autoid": 6081}, {"sensitive_word": "习近平", "autoid": 6080}, {"sensitive_word": "周永康", "autoid": 60791111}]
    # id已存在，就会报错如下的版本冲突
    # 'error': {'type': 'version_conflict_engine_exception',
    #           'reason': '[6081]: version conflict, document already exists (current version [2])'
    # sensitive_data_bulk_create(sensitive_data_list)
    crud_es_sensitive(sensitive_data_list, op_type="create")
    query_res = index_search_sense(query_ori)
    print(query_res)
    # sensitive_data_search(index_bulk_sense, "周永康")
    # res = es.get_source(index_bulk_sense, id='60791111')
    # document_data = es.get(index=index_bulk_sense, id=60791111, refresh=False)
    # print(type(document_data), document_data, sep="\n")


def crud_es_sensitive(sensitive_data_list, op_type):

    """
    :param sensitive_data_list: [{"sensitive_word": "习仲勋啊", "autoid": 6081}, {"sensitive_word": "习近平", "autoid": 6080}]
    :param op_type: 按照敏感词唯一id，即autoid进行增删改，"update"更新，"delete"删除，"create"新增
    :return: 无
    """
    # 解宇昊-敏感词的唯一id是autoid，作为删除的条件即可，很简单。
    # 增删改查，增加的时候，判断是否存在id，如果存在，则忽略
    # 删除，判断是否存在id，如果存在，则忽略
    res = "Successful"
    try:
        if op_type=="update":
            sensitive_data_bulk_update(sensitive_data_list)
        elif op_type=="delete":
            sensitive_data_bulk_delete(sensitive_data_list)
        elif op_type=="create":
            sensitive_data_bulk_create(sensitive_data_list)
        else:
            res = "Failed"
    except Exception as e:
        res = "Failed"
        print(e)
        print(traceback.format_exc())  # 返回异常信息的字符串，可以用来把信息记录到log里;

    return res


if __name__ == '__main__':
    main()
    # data_init()
    # data_update_bulk()
    # data_update_query()

