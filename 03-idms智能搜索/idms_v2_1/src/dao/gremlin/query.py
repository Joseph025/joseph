import sys
sys.path.append(r"/home/dc_dev/idms_v2/src")
from utils import common
from dao.gremlin import client
from dao.conf import db
from dao import err_info
import logging


'''
key:property name
value: property,data type:list
logic:"and" "or" "union"
isReg: is support regular filter
'''
class property():
    def __init__(self,key,value,logic,isReg):
        self.key   = key
        if common.data_is_NULL(value):
            self.value = []
        else:
            self.value = value
        self.logic = logic
        self.isReg = isReg
        

class edge():
    def __init__(self,label,direction,propertiesList):
        #propertiesList:list of class property()
        self.label          = label
        self.direction      = direction
        if common.data_is_NULL(propertiesList):
             self.propertiesList = []
        else:
            self.propertiesList = propertiesList
            
class node():
    def __init__(self,label,propertiesList):
        #propertiesList:list of class property()
        self.label          = label
        if common.data_is_NULL(propertiesList):
             self.propertiesList = []
        else:
            self.propertiesList = propertiesList
    

        
    
    
    
def get_common(cmd):
    return client.gremlin_client_req(cmd)
    
def get_label_by_name(name):
    cmd = "g.V().has('name','{0}').label()".format(name)
    isSucess,result = client.gremlin_client_req(cmd)
    if isSucess is not True:
        return isSucess,result

    value = result.get('data',[])

    return True,value
    
'''
function:querry_get_node_property_value(label,name,property)
return : alias list
'''
def get_node_property_value(label,name,propertyKey):
    cmd = "g.V()"
    if label is not None:
        cmd = cmd + ".hasLabel('{0}')".format(label)
    cmd = cmd + ".has('name','{0}').values('{1}')".format(name,propertyKey)
    logging.info("send: %s"%(cmd))
    isSucess,result = client.gremlin_client_req(cmd)
    if isSucess is not True:
        return isSucess,result
    value = result.get('data',[])

    return True,value


def get_node_by_name(label,name):
    cmd = "g.V()"
    if label is not None:
        cmd = cmd + ".hasLabel('{0}')".format(label)
    cmd = cmd + ".has('name','{0}')".format(name)
    cmd = cmd + ".dedup()"
    isSucess,result = client.gremlin_client_req(cmd)
    if isSucess is not True:
        return isSucess,result
    value = result.get('data',[])

    return True,value    
    
    
'''
function:get_nodes_by_label(label)
return : Node List,if the property of node has no value, then the property not in 'properites' dict keys
eg.
"code": "0",
"message": "执行gremlin查询",
"data": {
    "id": "90513a96-a7b9-4afc-ae67-ba6eabdda240",
    "data": [{
        "id": 36912,
        "label": "iPDTSwitch",
        "properties": {
            "pdtForm": ["CHASSIS"],
            "name": ["S12500-XS"],
            "alias": [ "12500xs","125", "125xs" ],
            "URL": ["http://www.h3c.com/cn/Products___Technology/Products/Switches/Data_Center_Switch/S12500/S12500-XS/"]}],
    "type": "VERTEX",
    "timeCost": 1264,
    "count": 83,
    "message": ""}}
'''    
def get_nodes_by_label(label):
    cmd = "g.V().hasLabel('{0}')".format(label)
    #logging.info("send: %s"%(cmd))
    return client.gremlin_client_req(cmd)
    


'''
function:_package_propertiesList_
input:propertiesList---data type:list of class property 
description: package propertiesList
'''
    
def _package_propertiesList_(propertiesList):
    property_str = ""
    for property in propertiesList:
        if common.data_is_NULL(property.value):
            continue
        if isinstance(property.value,list):
            for v in property.value:
                if property.isReg is True:
                    v_str = "has('{0}',textRegex('.*{1}.*')),".format(property.key,v)
                else:
                    v_str = "has('{0}','{1}'),".format(property.key,v)
                property_str = property_str + v_str
            property_str = property_str[:-1]
            property_str = "{0}(".format(property.logic) + property_str + ")"
        else:
            if property.isReg is True:
                property_str = "has('{0}',textRegex('.*{1}.*'))".format(property.key,property.value)
            else:
                property_str = "has('{0}','{1}')".format(property.key,property.value)

    return property_str
'''
function:get_nodes_by_properties
input   :label:node label
         propertiesList:unit data type:class property
''' 
def get_nodes_by_properties(label, propertiesList):
    #init cmd
    cmd = "g.V()"
    #split subject label
    if not common.data_is_NULL(label):
        label_str = "hasLabel('{0}')".format(label)
        cmd = cmd + "." + label_str
        
    #split property
    if not common.data_is_NULL(propertiesList):
        cmd = cmd +  '.' + _package_propertiesList_(propertiesList)
    
    #pirnt send cmd
    #logging.info("cmd: %s"%(cmd))
    return client.gremlin_client_req(cmd)
    
'''
function : get_node_name_by_property
eg.cmd:g.V().has('alias','iVerCMWV7B70').values('name')
'''
def get_node_name_by_property(label,propertyKey,propertyValue):
    cmd = "g.V()"
    if label is not None:
        cmd = cmd + ".hasLabel('{0}')".format(label)
    cmd = cmd + ".has('{0}','{1}').values('name')".format(propertyKey,propertyValue)
    logging.info("send: %s"%(cmd))
    isSucess,result = client.gremlin_client_req(cmd)
    if isSucess is not True:
        return isSucess,result
    nameList = result.get('data',None)
    if common.data_is_NULL(nameList):
        return False,None
    name = nameList[0]
    return True,name

    
'''
function:get_ndegree_object
input   :startNode:datatype:class node
         edgeList:datatype:class edge,list
         endNode:datatype:class node
         appendList:datatype:list,additional info
predicts:data type: class edge
''' 
def get_ndegree_object(startNode, \
                       edgeList, \
                       endNode, \
                       appendList = None):
    #init cmd  
    cmd = "g.V()" 
    
    #parse start node
    subjectLabel          = startNode.label
    subjectPropertiesList = startNode.propertiesList
    
    #split joint subjectLabel
    if not common.data_is_NULL(subjectLabel):
        s_label_str = "hasLabel('{0}')".format(subjectLabel)
        cmd = cmd + "." + s_label_str 
        
    #split joint subjectPorperty
    if not common.data_is_NULL(subjectPropertiesList):
        cmd = cmd + '.' + _package_propertiesList_(subjectPropertiesList)
        
    #split joint predict
    for edge in edgeList:
        if common.data_is_NULL(edge.direction):
            predictDirection = "both"
        else:
            predictDirection = edge.direction
            
        if common.data_is_NULL(edge.label):
            predict_str = "{0}()".format(predictDirection)
        else:
            predict_str = "{0}('{1}')".format(predictDirection,edge.label)
        cmd = cmd + "." + predict_str
     
    #parse start node
    objectLabel          = endNode.label
    objectPropertiesList = endNode.propertiesList
    
    #split joint objectLabel
    if not common.data_is_NULL(objectLabel):
        o_label_str = "hasLabel('{0}')".format(objectLabel)
        cmd = cmd + "." + o_label_str
    
    #split object property
    if not common.data_is_NULL(objectPropertiesList):
        cmd = cmd + "." +  _package_propertiesList_(objectPropertiesList)
    
        
    if not common.data_is_NULL(appendList):
        for append in appendList:
            cmd = cmd + "." + append
    
    #send query cmd    
    logging.info("cmd: %s"%(cmd))
    return client.gremlin_client_req(cmd)


    
def get_edges_by_label(label):
    cmd = "g.E().hasLabel('{0}')".format(label)
    logging.info("send: %s"%(cmd))
    return client.gremlin_client_req(cmd)

    
    
'''
function:get_edges_by_properties
input   :label,*propertyKey
''' 
def get_edges_by_properties(label, propertiesList):
    #init cmd
    cmd = "g.E()"
    #split subject label
    if not common.data_is_NULL(label):
        label_str = "hasLabel('{0}')".format(label)
        cmd = cmd + '.' + label_str
        
    #split property
    if not common.data_is_NULL(propertiesList):
        cmd = cmd + '.' +  _package_propertiesList_(propertiesList)
   
    #print send cmd    
    logging.info("cmd: %s"%(cmd))
    return client.gremlin_client_req(cmd)

    
'''
function:get_edges_by_properties
describe:get both 
input   :label,*propertyKey
''' 
def get_edges_bothv_by_properties(label, propertiesList):
    #init cmd
    cmd = "g.E()"
    #split subject label
    if not common.data_is_NULL(label):
        label_str = "hasLabel('{0}')".format(label)
        cmd = cmd + '.' + label_str
        
    #split property
    if not common.data_is_NULL(propertiesList):
        cmd = cmd + '.' +  _package_propertiesList_(propertiesList)
    
    #split bothv
    cmd = cmd + '.bothV()'
   
    #print send cmd    
    logging.info("cmd: %s"%(cmd))
    return client.gremlin_client_req(cmd)
    

    
'''
function:get_node_by_id
describe:get node by id 
input   :id
''' 
def get_node_by_id(id):
    #get url form DBConfig.ini,url no parameters loaded yet
    ret,cfg_url = db.get_vertex_by_id_url()
    if ret is not True:
        return ret,cfg_url
    
    #get url param
    ret,graphName = db.get_db_graph_name()
    if ret is not True:
        return ret,graphName

    urlDict = {'graphName' : graphName,
               'id'        : id}
    
    #replace url 
    url = common.url_param_replace(cfg_url,urlDict)
    
    #logging.info("url: " + url)

    #request data
    ret,data = client.request_get(url)
    if ret is not True:
        return False, data
        
    if common.data_is_NULL(data):
        logging.info("response data is None")
        return False,err_info.g_err_code.req_json_null

    result = data.get('result',None)
    if common.data_is_NULL(result):
        logging.info("response result is None")
        return False,err_info.g_err_code.req_json_null
        
    return True, result
    
'''
function:get_edge_by_id
describe:get edge by id 
input   :id
''' 
def get_edge_by_id(id):
    #get url form DBConfig.ini,url no parameters loaded yet
    ret,cfg_url = db.get_edge_by_id_url()
    if ret is not True:
        return ret,cfg_url
    
    #get url param
    ret,graphName = db.get_db_graph_name()
    if ret is not True:
        return ret,graphName

    urlDict = {'graphName' : graphName,
               'edgeId'    : id}
    
    #replace url 
    url = common.url_param_replace(cfg_url,urlDict)
    
    #logging.info("url: " + url)
    
    #request data
    ret,data = client.request_get(url)
    if ret is not True:
        return False, data

    if common.data_is_NULL(data):
        logging.info("response data is None")
        return False,err_info.g_err_code.req_json_null

    result = data.get('result',None)
    if common.data_is_NULL(result):
        logging.info("response result is None")
        return False,err_info.g_err_code.req_json_null
        
    return True, result
    
'''
function:get_edge_property_by_id
describe:get edge property value by id 
input   :id
''' 
def get_edge_property_by_id(id):
    #get url form DBConfig.ini,url no parameters loaded yet
    ret,cfg_url = db.get_edge_property_url()
    if ret is not True:
        return ret,cfg_url
    
    #get url param
    ret,graphName = db.get_db_graph_name()
    if ret is not True:
        return ret,graphName

    urlDict = {'graphName' : graphName,
               'edgeId'    : id}
    
    #replace url 
    url = common.url_param_replace(cfg_url,urlDict)
    
    #logging.info("url: " + url)

    #request data
    ret,data = client.request_get(url)
    if ret is not True:
        return False, data

    if common.data_is_NULL(data):
        logging.info("response data is None")
        return False,err_info.g_err_code.req_json_null

    result = data.get('result',None)
    if common.data_is_NULL(result):
        logging.info("response result is None")
        return False,err_info.g_err_code.req_json_null
        
    return True, result


