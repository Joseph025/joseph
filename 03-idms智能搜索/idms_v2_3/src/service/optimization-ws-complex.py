from flask import Flask,request,jsonify,Response
import requests
import json
import traceback
import logging
import logging.config
import configparser
from SortAlgorithm.model_interface import pred,init_model
import pickle
import hashlib

app = Flask(__name__)

#读取配置文件,ConfigParser用来解析ini配置文件
config = configparser.ConfigParser()
config.read("./config/config.ini",encoding="utf-8-sig")#


def init_log():
    global logger
    logging.config.fileConfig("./log/log-config")#读取日志配置文件
    logger = logging.getLogger()


def init_size():#检测ini文件中长度
    global SIZE
    SIZE = config.getint("OPTION","size")

def init():
    # 初始化加载算法模型
    #init_model()
    # 初始化日志
    init_log()
    # 初始化SIZE
    init_size()
    # POST请求ES
def call_es(req):
    response = requests.post(config.get("OPTION","url")+'?rest_total_hits_as_int='+'true',
                            headers={"content-type":"application/json; charset=UTF-8"},auth=('es7prodadmin','es7prodadmin!@#'),
                            data=json.dumps(req))
    return response.json()

# 对hits数组进行重排序
def sort_algorithm(hits,summary):
    # 调用算法模块进行排序，返回排序后的id
    hit_arr = pred(hits,summary)
    Dict = {}
    cnt = 0
    for Id in hit_arr:
        Dict[Id]=cnt
        cnt += 1
    # logger.info(Dict)
    # 根据排序后的id对hits数组重排序
    # new_hits = [0] * SIZE
    # for hit in hits:
    #     new_hits[Dict[hit["sort"][1]]]=hit
    return sorted(hits, key=lambda hit: Dict[hit["_id"]])

#判断是should 还是must
def isReqShould(req):
    if req["query"]["bool"].get("should") is not None:
            return True
    else:
            return False

#获得所有查询条件
def getAllMatchs(req):
    if isReqShould(req):
        return req["query"]["bool"]["should"]
    else:
        return req["query"]["bool"]["must"]

#找到用户请求的具体数值，可能是should，可能是must
def get_reqParameter(req, key):
    matchs = getAllMatchs(req)

    for match in matchs:
        if match["match"].get(key) is not None:
            return match["match"].get(key)
    return None

#删除Defect Number
def rm_defectNo(req):
    matchs = getAllMatchs(req)

    for index, match in enumerate(matchs):
        if match["match"].get("defectNo") is not None:
            del matchs[index]

@app.route("%s"%(config.get("OPTION","api")),methods=['POST'])  #/idms_defect
def optimization_ws():
    try:
        if request.json==None:
            return Response(u"400")

        if request.method =="POST":
            # 获取POST请求IDMS的参数
            req = request.json
            # 如果DefectNo 不是数字，做删除操作
            rps = call_es(req)
            return jsonify(rps)
        else:
            return "Request method Error: Please request again by POST"
    except Exception as e:
                logger.info(e)


if __name__ == '__main__':
    # 初始化
    init()
    app.run( host=config.get("OPTION","host"),
             port=config.getint("OPTION","port"),
             debug=True)#表示监听的端口，对收到的request运行生成response并返回。
