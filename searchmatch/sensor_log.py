import sensorsanalytics
import json
from searchhotmain.inition_config import config_result
# 在程序中初始化的代码段中构造神策分析 SDK 的实例：
# DefaultConsumer 是网络直接发送数据，如果网络出现异常可能会导致数据重发或丢失，因此不要用在任何线上服务中。线上请使用 ConcurrentLoggingConsumer。
# Consumer在配置文件中配置
# 从神策分析配置页面中获取数据接收的 URL
# SA_SERVER_URL = "https://testsensorsapi.juhaolian.cn/sa?project=default"
SA_SERVER_URL = config_result["SA_SERVER_URL"]
CONSUMER = config_result["CONSUMER"]
# sensor_search_log="/var/log/xwj-services/cloudbrain-search/sensorsearch/access.log"
sensor_search_log = config_result["sensor_search_log"]
SA_BULK_SIZE = 64  # 源码里面是list的长度

# 初始化一个 Consumer，用于数据发送
# DefaultConsumer 是同步发送数据，因此不要在任何线上的服务中使用此 Consumer
# 俊标厉害
if CONSUMER == "test":
    consumer = sensorsanalytics.DefaultConsumer(SA_SERVER_URL)
else:
    # "production"
    # consumer = sensorsanalytics.ConcurrentLoggingConsumer(SA_SERVER_URL)
    consumer = sensorsanalytics.ConcurrentLoggingConsumer(sensor_search_log, SA_BULK_SIZE)

# 使用 Consumer 来构造 SensorsAnalytics 对象
sa = sensorsanalytics.SensorsAnalytics(consumer)
# 记录用户登录事件
# ownerCode
# distinct_id = 'ABCDEF123456789'
# sa.track(distinct_id, 'UserLogin', is_login_id=True)
# sa.close()


def search_event_log(arg_dict, page_dict):
    # distinct_id = 'ABCDEF1234567'
    distinct_id = arg_dict['ownerCode']
    # result_page=json.loads(page_json)
    data=page_dict['data']
    searchRult=data['searchRult']
    if searchRult=='1':
        rn=len(data['spuCodes'])
    else:
        rn=0

    properties = {
        # 用户 IP 地址
        'key_word': arg_dict['query'],
        # 订单价格
        'result_number': rn,
    }
    # 记录用户订单付款事件
    sa.track(distinct_id, 'SearchRequest', properties, is_login_id=True)
    # print("yess")
    # sa.close()
    # 上报所有缓存数据，测试时可以调用调试数据，线上环境一般可以不调用
    # sa.flush()
