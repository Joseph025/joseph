import requests
from src.dao import err_info
import logging
from src.service import rest

HTTP_STATUS_CODE_OK = requests.codes.ok
GREMLIN_RESPONSE_CODE_OK = 200


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
