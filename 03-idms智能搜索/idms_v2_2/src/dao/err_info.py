#coding:utf-8
import sys
sys.path.append("/home/dc_dev/idms_v2/")


class err_info:
    def __init__(self,module=''):
        self.no_err             = ['0000', \
                                   module + 'No obvious mistakes', \
                                   module + '没有明显错误'] 
                                   
        self.argument_err       = ['1001', \
                                   module + 'argument is NULL', \
                                   module + '必要参数为空']
   
        self.req_timeout        = ['1002', \
                                   module + 'Internel Error:Requesting Data Timeout' , \
                                   module + '内部错误：请求数据超时 ']
                                   
        self.req_http_status_err = ['1003', \
                                   module + 'Internel Error:http status error' , \
                                   module + '内部错误:HTTP状态码错误']
                                   
        self.req_gremlin_code_err= ['1004', \
                                   module + 'Internel Error:gramlin platform resoponse code error' , \
                                   module + '内部错误:Gremlin数据平台返回的状态码错误']
                                   
        self.req_gremlin_success_err  = ['1005', \
                                   module + 'Internel Error:gramlin platform resoponse error' , \
                                   module + '内部错误:Gremlin数据平台返回的success码错误']
                                   
        self.req_config_file_err = ['1006', \
                                   module + 'Internel Error:get local config file error' , \
                                   module + '内部错误:读取本地配置文件错误']
                                   
        self.req_json_none      = ['1007', \
                                   module + 'gremlin response json is NULL', \
                                   module + 'Gremlin数据平台返回数据为空']
        self.kafka_err          = ['1008', \
                                   module + 'kafka send error', \
                                   module + 'kafka写队列错误']
        self.cons_extra_err     = ['2000', \
                                   module + 'construction extraction error', \
                                   module + '知识抽取故障']
        self.cons_kdd_err       = ['2001', \
                                   module + 'Knowledge Discovery in Database error', \
                                   module + '知识挖掘故障']
        self.cons_kr_err        = ['2002', \
                                   module + 'Knowledge Representation error', \
                                   module + '知识表示故障']
        self.cons_store_err     = ['2003', \
                                   module + 'Knowledge store error', \
                                   module + '知识存储故障']
                                   

                                   
                                   
g_err_code = err_info()