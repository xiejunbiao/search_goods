import requests
import json

url = "http://10.18.226.38:9001/mysql/query"


def getDataFromSqyn():
    data = {
        "platform": "4",
        "sql": '''select * from cb_goods_spu_search where goods_status=1 and store_status=1'''
    }
    j = requests.post(url, data=data).json()
    print("导出数据成功，开始获取小区信息")
    return json.loads(j)


def getAreaForGoods(spuCode):
    data = {
        "platform": "4",
        "sql": '''select area_code from cb_goods_scope where spu_code="{}"'''.format(spuCode)
    }
    j = requests.post(url, data=data).json()
    areaList = [i["area_code"] for i in json.loads(j)]
    return areaList


def insert(goods):
    esUrl = "http://10.18.226.38:9200/sqyntest/goods/_bulk"
    data = []
    for g in goods:
        data.append('''{"index":{}}\n''' + json.dumps(g, ensure_ascii=False)+"\n")
    payload="".join(data)+"\n"
    print(payload)
    r=requests.post(url=esUrl,data=payload.encode(),headers={"Content-Type":"application/json;charset=UTF-8"}).json()
    print(r)


def insertDataToEs(goods):
    # 每1000条导入一次
    pages = len(goods) // 1000
    left = len(goods) % 1000
    if pages < 1:
        insert(goods)
        return
    lastPage = 0
    for i in range(pages):
        lastPage = i
        start = i * 1000
        end = start + 1000
        insert(goods[start:end])
    start = (lastPage + 1) * 1000
    end = start + left
    insert(goods[start:end])

    # requests.post(esUrl, json=good)


if __name__ == '__main__':
    goods = []
    for good in getDataFromSqyn():
        good["area"] = getAreaForGoods(good["spu_code"])
        goods.append(good)
    print("填充小区完成，开始导入es")
    insertDataToEs(goods)
    # print(13634 // 1000)
