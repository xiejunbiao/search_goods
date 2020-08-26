# -*- coding: utf-8 -*-
"""
Created on Mon May  4 10:55:42 2020

@author: lijiangman
"""
import configparser
import sys,getopt
import platform


class ConfigValue:
    # 项目路径
    # rootDir = os.path.split(os.path.realpath(__file__))[0]
    # config.ini文件路径
    # configFilePath = os.path.join(rootDir, 'config.ini')
    def __init__(self, rootDir):
        # configFilePath = os.path.join(rootDir, 'config.ini')
        configFilePath = rootDir
        self.config = configparser.ConfigParser()
        self.config.read(configFilePath)
        # print(configFilePath)

    def get_config_values(self):
        """
        根据传入的section获取对应的value
        :param section: ini配置文件中用[]标识的内容
        :return:
        """
        # return config.items(section=section)
        # argList=['ip','user','password','db','port','envIp','tb_goods_spu_search','tb_goods_scope','tb_hot_search_words','cb_shop_rank_info']
        argList = ['ip', 'user', 'password', 'db', 'port', 'tb_goods_spu_search', 'tb_goods_scope',
                   'tb_hot_search_words', 'tb_sensitive_words', 'cb_shop_rank_info']
        config_result={}
        section = 'mysql'
        for option in argList:
            if option=='port':
                config_result['port']=int(self.config.get(section=section, option=option))
            else:
                config_result[option]=self.config.get(section=section, option=option)

        argList_sensor=['SA_SERVER_URL', 'CONSUMER', "sensor_search_log"]
        section="sensor"
        for option in argList_sensor:
            config_result[option]=self.config.get(section=section, option=option)

        argList_sensor=['es_host1', 'es_host2', "es_host3", "cb_es_sensitive_words"]
        section="es"
        for option in argList_sensor:
            config_result[option]=self.config.get(section=section, option=option)

        return config_result


def para_set(argv):
    ip = ''
    port = ''
    try:
        opts, args = getopt.getopt(argv, "hc:l:d", ["conf=", "log=", "db="])
    except getopt.GetoptError:
        print('test.py -i <inputfile> -p <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        print(opt, arg)
        if opt == '-h':
            print('test.py -i <inputfile> -p <outputfile>')
            sys.exit()
        elif opt in ("-c", "--conf"):
            conf = arg
        elif opt in ("-l", "--log"):
            log = arg
        elif opt in ("-d", "--db"):
            db = arg

    #   self.__logger.info('输入的文件为：', ip)
    #   self.__logger.info('输出的文件为：', port)
    #   self.__logger.info('db:',db)
    # return (conf,log)
    return (conf, log)


sys_type=platform.system()

if sys_type=="Linux":
    # 命令行传参--Linux
    argv=sys.argv[1:]
    (confPath,log)=para_set(argv)
elif sys_type=="Windows":
    # 变量赋值--windows
    log=r'D:\hisense\speechR\searchpkg\searchgoodshttp\log\stdoutOnlineEnv.log'
    # log=r'D:\hisense\speechR\searchpkg\searchgoodshttp\log\stdout.log.cat'
    # confPath=r'D:\hisense\speechR\searchpkg\searchgoodshttp\conf\config58_sc.ini'##生产环境
    confPath = r'D:\hisense\speechR\searchpkg\searchgoodshttp\conf\config58.ini'
else:
    # 命令行传参--Linux
    argv = sys.argv[1:]
    (confPath, log) = para_set(argv)

# confPath=r'D:\hisense\speechR\searchpkg\searchgoodshttp\server\config58_test.ini'##实验增量更新
# confPath=r'D:\hisense\speechR\searchpkg\searchgoodshttp\server\config114.ini'#线上数据清洗
# confPath=r'D:\hisense\speechR\searchpkg\searchgoodshttp\server\config99.ini'
################################################
cv=ConfigValue(confPath)
config_result=cv.get_config_values()  # 全局变量，运行一次，数据库配置。放到内存里面
config_result['log']=log

if __name__=='__main__':
   
    confPath=r'D:\hisense\speechR\searchpkg\searchgoodshttp\conf\config58.ini'
    cv=ConfigValue(confPath)
    config_result=cv.get_config_values()
    print(config_result)
