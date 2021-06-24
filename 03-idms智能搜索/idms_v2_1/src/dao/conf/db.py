#file_name    :db.py
#file_function:paraser cfg/ ini file
#author       :liumengemeng 13195
#date         :20200417

import sys
#sys.path.append(r"/home/l13195/issueSearch/KG/src")
import os
import configparser
from src.dao import err_info
from src.utils import common
import logging


_DB_URL_FILE_DIR  = os.path.join(os.path.dirname(os.getcwd()), \
                             'root', \
                             'cfg', \
                             'DBConfig.ini')


def _get_conf_parser_(filename):
    if os.path.exists(filename):
        parser = configparser.ConfigParser()
        parser.read(filename)
        return True, parser
    else:
        logging.info('DB config file not exist')
        return False, err_info.g_err_code.req_config_file_err


def _split_joint_url_(addr,url):
    return 'http://{0}{1}'.format(addr,url)


def _get_ini_url_common_(filename, \
                         proparty_class, \
                         proparty_value):
    ret,value = _get_conf_parser_(filename)
    if ret is not True:
        return ret, value
    else:
        ip_port = value.get('server','address')
        uri = value.get(proparty_class,proparty_value)
        url = _split_joint_url_(ip_port,uri)
        return True, url


def _get_ini_common_(filename, \
                     proparty_class, \
                     proparty_value):
    ret,value = _get_conf_parser_(filename)
    if ret is not True:
        return ret, value
    else:
        return True, value.get(proparty_class,proparty_value)


def get_db_graph_name():
    return _get_ini_common_(_DB_URL_FILE_DIR,'graph','graphName')


def get_cluster_kafka_addr_str():
    ret,kafka_port = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','kafka_port')
    if ret is not True:
        return ret,kafka_port
    ret,host1 = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host1_addr')
    if ret is not True:
        host1_kafka = ''
    else:
        host1_kafka = host1 + ":" + kafka_port 
    ret,host2 = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host2_addr')
    if ret is not True:
        host2_kafka = ''
    else:
        host2_kafka = host2 + ":" + kafka_port 
    ret,host3 = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host3_addr')
    if ret is not True:
        host3_kafka = ''
    else:
        host3_kafka = host3 + ":" + kafka_port 
    
    addr_str = host1_kafka + ',' + host2_kafka + ',' + host3_kafka
    return addr_str
    
# def get_cluster_hosts_conf_list():
#     #todo exception handling
#     conf_list = []
#     _,local_area = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','local_area')
#     _,kgde1_addr = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host1_addr')
#     _,kgde1_dns  = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host1_dns')
#     _,kgde1_name = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host1_name')
#
#     _,kgde2_addr = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host2_addr')
#     _,kgde2_dns  = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host2_dns')
#     _,kgde2_name = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host2_name')
#
#     _,kgde3_addr = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host3_addr')
#     _,kgde3_dns  = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host3_dns')
#     _,kgde3_name = _get_ini_common_(_DB_URL_FILE_DIR,'cluster','host3_name')
#
#     conf_list.append(kgde1_addr + " " + kgde1_dns + " " + kgde1_name + " " + local_area)
#     conf_list.append(kgde2_addr + " " + kgde2_dns + " " + kgde2_name)
#     conf_list.append(kgde3_addr + " " + kgde3_dns + " " + kgde3_name)
#
#     return conf_list
#
    
    
    

'''
client
'''    
def get_germlin_url():
    return _get_ini_url_common_(_DB_URL_FILE_DIR,'url_client','clientGremlin')


def get_germlinUI_url():
    return _get_ini_url_common_(_DB_URL_FILE_DIR,'url_client','clientGremlinUI')

'''
vertex
'''    
def get_vertex_create_url():
    return _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexCreate')
    
def get_vertex_by_id_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexGetbyId')
    
def get_vertex_property_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexGetProperty')
    
def get_vertex_update_property_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexUpdateProperty')
 
def get_vertex_delete_property_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexDeleteProperty')
    
def get_vertex_delete_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexDelete')
    
def get_vertex_delete_batch_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexDeleteBatch')
    
def get_vertex_count_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_vertex','vertexCount')


'''
edge
'''    
def get_edge_create_url():
    return _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeCreate')
    
def get_edge_by_id_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeGetbyId')
    
def get_edge_property_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeGetProperty')
    
def get_edge_update_property_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeUpdateProperty')
 
def get_edge_delete_property_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeDeleteProperty')
    
def get_edge_delete_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeDelete')
    
def get_edge_delete_batch_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeDeleteBatch')
    
def get_edge_count_url():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_data_edge','edgeCount')


'''
schema vertex
'''     
def get_schema_vertex_lable():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_vertex','schemaVertexLabelsGet')

def get_schema_vertex_label_create():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_vertex','schemaVertexLabelsCreate')

def get_schema_vertex_label_update():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_vertex','schemaVertexLabelsUpdate')

def get_schema_vertex_label_delete():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_vertex','schemaVertexLabelsDelete')

def get_schema_vertex_label_specified():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_vertex','schemaVertexLabelsSpecified')

def get_schema_vertex_label_property_add():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_vertex','schemaVertexLabelsPropertyAdd')
    
'''
schema edge
'''     
def get_schema_edge_lable():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_edge','schemaEdgeLabelsGet')

def get_schema_edge_label_create():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_edge','schemaEdgeLabelsCreate')

def get_schema_edge_label_update():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_edge','schemaEdgeLabelsUpdate')

def get_schema_edge_label_delete():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_edge','schemaEdgeLabelsDelete')

def get_schema_edge_label_specified():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_edge','schemaEdgeLabelsSpecified')

def get_schema_edge_label_property_add():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_edge','schemaEdgeLabelsPropertyAdd')
    
def get_schema_edge_label_connect():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_edge','schemaEdgeLabelsConnect')


'''    
schema properity
'''
def get_schema_properity_create():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_property','schemaPropertyCreate')

def get_schema_properity_update():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_property','schemaPropertyUpdate')

def get_schema_properity_delete():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_property','schemaPropertyDelete')

def get_schema_properity_specified():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_property','schemaPropertySpecifiedGet')

def get_schema_properity():
    return  _get_ini_url_common_(_DB_URL_FILE_DIR,'url_schema_property','schemaPropertyGet')

    



    


    