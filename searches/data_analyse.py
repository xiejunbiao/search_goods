# Licsensed to Elasticsearch B.V under one or more agreements.

"""
Script that process the cook csv file and streams it to an Elasticsearch cluster
Reference gitHub.com/elastic/elasticsearch-py/blob/master/examples/bulk-ingest/bulk-ingest.py
"""

import csv, re
from os.path import abspath, join, dirname, exists
from elasticsearch import Elasticsearch
from searchhotmain.inition_db import SelectMysqlDatabase
from searchhotmain.inition_config import config_result  # 数据库配置

# from elasticsearch.helpers import streaming_bulk

DATASET_PATH_SENSE = join(dirname(abspath(__file__)), "filesmg", "chinese_dictionary.txt")  # 敏感词

DATASET_PATH_COOK = join(dirname(abspath(__file__)), "filesmg", "air_result.csv")

DATASET_PATH_COOK_NEW = join(dirname(abspath(__file__)), "filesmg", "cook_results_new.csv")

cook_data = [{"id": 125, "name": "丝瓜炒鸡蛋", "ingredients": "丝瓜 小米椒", "methods": "1、丝瓜切去头尾，刮去皮，冲洗干净。2、将丝瓜切成滚刀块的。"},
             {"id": 138, "name": "香菇烧大排", "ingredients": "大排，香菇，姜，鸡蛋",
              "methods": "1、大排洗净沥干用肉锤敲打后，加入适量盐，胡椒粉，生抽及食用油腌制一小时。2、香菇洗净泡发好撕成小块备用的。"}]


def generate_actions_sense():
    """全量初始化 Read the file through csv.DictReader() and """

    # with open(DATASET_PATH_SENSE, mode='r', encoding='UTF-8') as f:
    #     for line in f.readlines():
    #         line=line.strip("\n")
    #         if line == '':
    #             continue
    #         doc = {
    #             "type_s": "sense",
    #             "keyword_s": line,
    #         }
    #         yield doc

    """ mysql全量初始化 """
    # sensitive_words = SelectMysqlDatabase().query_for_sensitive_words()
    dataDf = SelectMysqlDatabase().query_for_sensitive_words()
    for index, row in dataDf.iterrows():
        sensitive_word = row["sensitive_word"]
        # 为了防止数字被屏蔽，尤其是商品详情页里面的图片的地址，不屏蔽数字
        # if sensitive_word == '' or bool(re.search(r'\d', sensitive_word)):
        # 为了逃避责任，还是不屏蔽数字了
        if sensitive_word == '':
            continue
        doc_s = {
            '_op_type': 'index',  # 默认是index
            "_id": row["autoid"],  # mysql唯一自增id
            "type_s": "sense",
            "sensitive_word": sensitive_word,
        }
        yield doc_s


def generate_actions_sense_test():
    """全量初始化 Read the file through csv.DictReader() and """

    with open(DATASET_PATH_SENSE, mode='r', encoding='UTF-8') as f:
        for line in f.readlines():
            line=line.strip("\n")
            if line == '':
                continue
            doc = {
                "type_s": "sense",
                "sensitive_word": line,
            }
            yield doc




def csv_dict_writer():
    with open(DATASET_PATH_COOK, 'r', encoding="utf-8") as f:
        for i in f.readlines():
            print(i)
        # data=csv.reader(f)
        # for i in data:
        #     for j in i:
        #         print(j.decode('gbk'))

    # pass

    # with open(DATASET_PATH_COOK_NEW, ’w’) as csvfile:
    #     writer = csv.DictWriter(csvfile,fieldnames=[‘id’,’class’])
    #     #写入列标题，即DictWriter构造方法的fieldnames参数
    #     writer.writeheader()
    #     for data in datas:
    #         writer.writerow({‘id’:data[0],’class’:data[1]})


def generate_actions2():
    """全量初始化 Read the file through csv.DictReader() and """

    with open(DATASET_PATH_COOK, mode='r', encoding='UTF-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            print(row)
            doc = {
                "_id": row["id"],
                "name": row["name"],
                "ingredients": row["ingredients"],
                "methods": row["methods"]
            }
            yield doc


def generate_actions():
    """全量初始化 Read the file through csv.DictReader() and """

    for row in cook_data:
        doc = {
            "_id": row["id"],
            "name": row["name"],
            "ingredients": row["ingredients"],
            "methods": row["methods"]
        }
        yield doc


def test_anaylyse():
    # es = Elasticsearch([{"host": "10.18.226.38", "port": 9200}])
    es_host = [config_result['es_host1'], config_result['es_host2'], config_result['es_host3']]
    es = Elasticsearch(hosts=es_host,
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
    body={
        "text": "一群傻逼",
        # "analyzer": "ik_max_word",
        "analyzer": "my_analyzer",
        # "analyzer": "keyword",
        # "analyzer": "standard"
    }
    res = es.indices.analyze(body=body, index="test_env_index_sense_word_16")
    print(res)


if __name__ == "__main__":
    # csv_dict_writer()
    # for i in generate_actions():
    #     print(i)
    # for i in generate_actions_sense():
    #     print(i)
    test_anaylyse()
