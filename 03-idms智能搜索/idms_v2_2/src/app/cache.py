import sys
sys.path.append("/home/dc_dev/idms_v2/")
import re
import threading
from src.dao.gremlin import query
from src.utils import common
import logging

cache_lock = threading.Lock()

# def jieba_load_userdict():
#     wordSegment.jieba_load_userdict()
#

g_entity_dict = {
    "iDefect"         :{},
    "iICMS_Ext"       :{},
    "iICMS_Network"   :{},
    "iConfig"         :{},
    "iFeatureV7B70"   :{},
    "iPDTSwitchSeries":{},
    "iPDTSwitchSpecs" :{},
    "iPDTSwitch"      :{},
    "iPDTPL"          :{},
    "iPDTH3C"         :{},
    "iCustomer"       :{},
    "iVerSwitchBranch":{},
    "iVerSwitchBranchDemo" :{},
    "iVerSwitchBranchARAD" :{},
    "iVerSwitchBranchIRF3" :{},
    "iVerSwitchBranchJR" :{},
    "iVerCMWV7Branch" :{},
    "iVerCMWV7BranchDemo":{},
    "iVerCMWV7B23" :{},
    "iVerCMWV7B35" :{},
    "iVerCMWV7B45" :{},
    "iVerCMWV7B70" :{}
}

g_ignore_instance_name = ['alias', \
                          'display', \
                          '?',\
                          'repeat', \
                          'return', \
                          'description', \
                          'send', \
                          'telnet', \
                          'shutdown', \
                          'speed', \
                          'default', \
                          'mtu', \
                          'clock', \
                          'flag', \
                          'bandwidth', \
                          'service']
'''
function:req
describe:load DB data to local memroy
input   :data label list
return  :global g_entity_dict
'''

def req(keyList):

    global g_entity_dict
    
    cache_lock.acquire()
    
    for k in list(keyList):
        cmd = "g.V().hasLabel('{0}').valueMap('name','alias')".format(k)

        ret,result = query.get_common(cmd)

        if ret is not True:
            logging.info("get <%s> entity is NULL"%k)
            continue
        data = result.get('data')
        if common.data_is_NULL(data):
            logging.info("get <%s> entity is NULL"%k)
            continue

        for d in data:
            if "alias" in d:
                name = re.findall(r"name=\[(.+?)\],",d)
            else:
                name = re.findall(r"name=\[(.+?)\]\}",d)
            if common.data_is_NULL(name):
                continue
            name = common.list_str_2_list(name[0],', ')

            alias = re.findall(r"alias=\[(.+?)\]\}",d)
            if not common.data_is_NULL(alias):

                alias = common.list_str_2_list(alias[0],', ')
            g_entity_dict[k].update({name[0]:alias})
            
    cache_lock.release()

    return
    
# def req_r(keyList):
#     global g_relation_dict
#
#     cache_lock.acquire()
#     for k in keyList:
#         cmd = "g.V().hasLabel('{0}').as('a')" \
#                     ".out('{1}')" \
#                     ".hasLabel('{2}').as('b')" \
#                     ".select('a','b')" \
#                     ".by('name')".format(g_relation_dict[k]["startLabel"],k,g_relation_dict[k]["endLabel"])
#         ret,result = query.get_common(cmd)
#         if ret is not True:
#             logging.info("get <%s> entity is NULL"%k)
#             continue
#         data = result.get('data')
#         if common.data_is_NULL(data):
#             logging.info("get <%s> entity is NULL"%k)
#             continue
#
#         for d in data:
#             #get startnode name
#             startName = re.findall(r"a=(.+?)\,",d)[0]
#             if common.data_is_NULL(startName):
#                 continue
#             #get endnode name,need longest match,so pattern need not '?'
#             endName = re.findall(r"b=(.+)\}",d)[0]
#             if common.data_is_NULL(endName):
#                 continue
#             #update g_relation_dict
#             if endName in g_relation_dict[k]['data'].keys():
#                 value = g_relation_dict[k]['data'][endName]
#                 if isinstance(value,list):
#                     value.append(startName)
#                 else:
#                     value = [value]
#                     value.append(startName)
#                 g_relation_dict[k]['data'].update({endName:value})
#             else:
#                 g_relation_dict[k]['data'].update({endName:startName})
#
#     cache_lock.release()
#
#     return
#
#
'''
function:probe
describe:Maintenans g_entity_dict data,collect label which has no data
input   :
return  :some g_entity_dict key(label),data type is list
'''
# def probe():
#     instanceList = []
#     relationList = []
#     global g_entity_dict
#     for k in g_entity_dict.keys():
#         if common.data_is_NULL(g_entity_dict[k]):
#             instanceList.append(k)
#             logging.info("<%s> data in g_entity_dict is Null"%k)
#     for k in g_relation_dict.keys():
#         if common.data_is_NULL(g_relation_dict[k]["data"]):
#             relationList.append(k)
#             logging.info("<%s> data in g_relation_dict is Null"%k)
#     return instanceList,relationList
#

'''
function:get
describe:get instance data from local memory
input   :label--string,data label,
return  :global g_entity_dict
'''    
def get(label):
    global g_entity_dict

    if not label in g_entity_dict.keys():
        g_entity_dict.update({label:{}})

    if common.data_is_NULL(g_entity_dict[label]):
        req([label])
        
    return g_entity_dict[label]
 
'''
function:get_r
describe:get relation data from local memory
input   :label--string,data label,
         isReverse---bool true:key:innodename,value:outnodename
                          false:key:outnodename,value:innodename
return  :global g_entity_dict
''' 
# def get_r(label,isReverse=True):
#     global g_relation_dict
#     if common.data_is_NULL(g_relation_dict[label]["data"]):
#         req_r([label])
#     if isReverse:
#         return g_relation_dict[label]["data"]
#     else:
#         srcDict = g_relation_dict[label]["data"]
#         return common.reverse_dict(srcDict)


'''
function:monitor_daily
describe:cache refresh every day
input   :
return  :
'''
# def monitor_daily():
#     global g_entity_dict
#     global g_relation_dict
#
#     #refresh cache instance data
#     instanceList = g_entity_dict.keys()
#     req(instanceList)
#     #refresh cache relation data
#     relationList = g_relation_dict.keys()
#     req_r(relationList)
#     #timer
#     threading.Timer(60*60*24, monitor_timer).start()
#
#
# def monitor_timer():
#     instanceList,relationList = probe()
#     if not common.data_is_NULL(instanceList):
#         req(instanceList)
#     if not common.data_is_NULL(relationList):
#         req_r(relationList)
#     threading.Timer(60*60, monitor_timer).start()
#
#
#
    