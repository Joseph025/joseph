import sys
sys.path.append(r"/home/dc_dev/idms_v2/src")
import requests
import json
from requests.exceptions import ReadTimeout
from dao import err_info
import logging
from service import rest

HTTP_STATUS_CODE_OK = requests.codes.ok
GREMLIN_RESPONSE_CODE_OK = 200


'''
avoid BigDataKnowledgegraph platform bug,need change some specaial symbols
naming rules for special symbols:
h3cresearch + synbol chinese pin yin + mask
'''
g_content_mask_dict = {
    '<':'h3cresearchzuojiankuohaomask',
    '>':'h3cresearchyoujiankuohaomask'}


'''
function:data_mask_process
describe:in order to aviod BigDataKnowledgegraph platform bug,need change some specaial symbols
input:json data
return:processed string data
'''	
def data_mask_process(data_dict):
    data_string = json.dumps(data_dict)
    for key in g_content_mask_dict.keys():
        data_string = data_string.replace(key,g_content_mask_dict[key])
    return data_string

def request_post(url,content):
    headers = {"Content-Type": "application/json"}
    logging.info("url encode: " + url)
    #Logger.logger.info("content:%s"%content)
    data = data_mask_process(content)
    try:
        responsedata = requests.post(url=url, data=data, headers=headers)
    except ReadTimeout:
        logging.info("url %s"%(url))
        return False, err_info.g_err_code.req_timeout
    else:
        response = responsedata.json()
        status = responsedata.status_code
    #analysis return code
    if HTTP_STATUS_CODE_OK != status:
        logging.info("status %s"%(str(status)))
        return False, err_info.g_err_code.req_http_status_err

    if response.get('code') != GREMLIN_RESPONSE_CODE_OK:
        logging.info("response code is not_GREMLIN_RESPONSE_CODE_OK ")
        return False, err_info.g_err_code.req_gremlin_code_err

    if response.get('success') is not True:
        logging.info("value of 'success' in gremlin response is not true")
        return False, err_info.g_err_code.req_gremlin_success_err


    return True, response.get('data',{})

    
'''
function  :gremlin_client_req
input     :cmd
DBResponse:
{
    "code": "0",
    "message": "执行 gremlin 查询",
    "data": {
        "createTime": 1577674601,
        "updateTime": 1577674601,
        "queryInfo": {
            "id": "default_coralgraph",
            "graphName": "default_airports_graph",
            "gremlin": "g.V()"
        },
        "result": {
            "id": "ff0bd3ae-20e6-4ab6-9e66-faa5a6191a2d",
            "data": [
                {
                    "id": 4096,
                    "label": "airport",
                    "properties": {
                        "country": "US",
                    },
                    "group": "airport"
                }
            ],
            "type": "VERTEX",
            "timeCost": 1192,
            "count": 1,
            "message": "Partial 1 records are shown.",
            "graph": {
                "vertices": [
                    {
                        "id": 4096,
                        "label": "airport",
                        "properties": {"country": "US"},
                        "group": "airport"
                    }
                ],
                "edges": [
                    {
                        "id": "latu-391s-pat-3fao",
                        "label": "rVersionhasChild",
                        "outVertex": {
                            "id": 151696,
                            "label": "iVerCMWV7BranchDemo",
                            "properties": {},
                            "group": "iVerCMWV7BranchDemo"
                        },
                        "inVertex": {
                            "id": 159792,
                            "label": "iVerCMWV7BranchDemo",
                            "properties": {},
                            "group": "iVerCMWV7BranchDemo"
                        },
                        "properties": {
                            "Range": "iVerCMWV7BranchDemo",
                            "Domain": "iVerCMWV7BranchDemo"
                        }
                    }
                ]
            }
        }
    }
}
return    :
        ret----True or False
        result-DBResponse["data"]["result"]
'''
def gremlin_client_req(cmd):
    reponseData = rest.HgeClient("10.165.8.92", "dc_dev", "1qazxsw@").get("hge-graphdb", "/api/graphs/dc_idms/gremlin?gremlin="+cmd+"")
    response=reponseData.json()
    status  = reponseData.status_code

    if HTTP_STATUS_CODE_OK != status:
        logging.info("status %s" % (str(status)))
        return False, err_info.g_err_code.req_http_status_err

    # if response.get('code') != GREMLIN_RESPONSE_CODE_OK:
        # logging.info("response code is not_GREMLIN_RESPONSE_CODE_OK ")
        # return False, err_info.g_err_code.req_gremlin_code_err

    return True, response.get('data')
