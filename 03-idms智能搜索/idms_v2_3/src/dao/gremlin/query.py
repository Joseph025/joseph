from src.dao.gremlin import client


    
def get_common(cmd):
    return client.gremlin_client_req(cmd)


