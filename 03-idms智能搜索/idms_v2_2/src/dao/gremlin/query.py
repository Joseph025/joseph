import sys
sys.path.append("/home/dc_dev/idms_v2/")
from src.dao.gremlin import client


    
def get_common(cmd):
    return client.gremlin_client_req(cmd)


