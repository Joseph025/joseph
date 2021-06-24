import sys
sys.path.append(r"/home/dc_dev/idms_v2/src")
from utils import common
import logging
import re
from bs4 import BeautifulSoup
from service import wordSegment
from app import cache
from service import rest
from dao import issueconstruction
import operator
from functools import reduce

g_align_label = ['iConfig',
    'iFeatureV7B70',
    'iCustomer',
    'iPDTPL',
    'iPDTSwitch',
    'iPDTSwitchSeries',
    'iPDTSwitchSpecs',
	'iPDTSwitch',
    'iPDTPL',
    'iPDTRouter',
    'iPDTRouterSeries',
    'iPDTRouterSpecs',
    'iPDTNew_network',
    'iPDTNew_networkSeries',
    'iPDTNew_networkSpecs',
    'iPDTIP_Wlan',
    'iPDTIP_WlanSeries',
    'iPDTIP_WlanSpecs',
    'iPDTH3C_Soft',
    'iPDTH3C_SoftSeries',
    'iPDTH3C_SoftSpecs',
    'iPDTIP_Security',
    'iPDTIP_SecuritySeries',
    'iPDTIP_SecuritySpecs',
    'iPDTServer',
    'iPDTServerSeries',
    'iPDTServerSpecs',
    'iPDTStorage',
    'iPDTStorageSeries',
    'iPDTStorageSpecs',
    'iPDTFusion_Architecture',
    'iPDTFusion_ArchitectureSeries',
    'iPDTFusion_ArchitectureSpecs',
    'iPDTH3Cloud',
    'iPDTH3CloudSeries',
    'iPDTH3CloudSpecs',
    'iPDTBig_Data',
    'iPDTBig_DataSeries',
    'iPDTBig_DataSpecs',
    'iPDTIoT',
    'iPDTIoTSeries',
    'iPDTIoTSpecs',
    'iPDTBig_Security',
    'iPDTBig_SecuritySeries',
    'iPDTBig_SecuritySpecs',
    'iPDTLTE',
    'iPDTLTESeries',
    'iPDTLTESpecs',
    'iPDTTransmission',
    'iPDTTransmissionSeries',
    'iPDTTransmissionSpecs',
    'iPDTAngle',
    'iPDTAngleSeries',
    'iPDTAngleSpecs',
    'iPDTCabling',
    'iPDTCablingSeries',
    'iPDTCablingSpecs',
    'iPDTH3CloudOS',
    'iPDTH3CloudOSSeries',
    'iPDTH3CloudOSSpecs',
    'iPDTIntelligence_Center',
    'iPDTIntelligence_CenterSeries',
    'iPDTIntelligence_CenterSpecs',
    'iPDTIntelligence_Home',
    'iPDTIntelligence_HomeSeries',
    'iPDTIntelligence_HomeSpecs',
    'iPDTMini',
    'iPDTMiniSeries',
    'iPDTMiniSpecs',
    'iPDTStandard_Network',
    'iPDTStandard_NetworkSeries',
    'iPDTStandard_NetworkSpecs',
    'iPDTH3C',
    'iPDTSpecsProperty_Switchs',
    'iPDTSpecsProperty_Router',
    'iPDTSpecsProperty_NewNetwork',
    'iPDTSpecsProperty_IPWlan',
    'iPDTSpecsProperty_H3CSoft',
    'iPDTSpecsProperty_IPSecurity',
    'iPDTSpecsProperty_Server',
    'iPDTSpecsProperty_Storage',
    'iPDTSpecsProperty_FusionArchitecture',
    'iPDTSpecsProperty_H3Cloud',
    'iPDTSpecsProperty_BigData',
    'iPDTSpecsProperty_IoT',
    'iPDTSpecsProperty_BigSecurity',
    'iPDTSpecsProperty_LTE',
    'iPDTSpecsProperty_Transmission',
    'iVerCMWV7Branch',
    'iVerCMWV7B23',
    'iVerCMWV7B35',
    'iVerCMWV7B45',
    'iVerCMWV7B70',
    'iVerH3C',
    'iWordDict',
    'iRealation']


'''
key:alias,value:name
'''
g_primary_key_align_dict = {}


def words_white_list_process(whiteListFile, words):
    retList = []
    if common.data_is_NULL(whiteListFile):
        return words
    whiteList = common.read_file_lines_to_list(whiteListFile)

    if common.data_is_NULL(whiteList):
        return []
    # Logger.logger.info("whiteList:%s"%(common.list_2_str(whiteList)))
    for d in words:
        if len(str(d))>0:
            # 如果索引的词在白名单里
            if d in whiteList:
                # 如果在白名单里不在retList里
                if d not in retList:
                    retList.append(d)
                else:
                    continue
    # Logger.logger.info("after whiteList:%s"%(common.list_2_str(retList)))
    return retList


def words_black_list_process(blackListFile, words):
    retList = []
    if common.data_is_NULL(blackListFile):
        return words
    blackList = common.read_file_lines_to_list(blackListFile)
    if common.data_is_NULL(blackList):
        return words
    # Logger.logger.info("blackList:%s"%(common.list_2_str(blackList)))
    for d in words:
        if d in blackListFile:
            continue
        if d in retList:
            continue
        retList.append(d)
    # Logger.logger.info("after blackList:%s"%(common.list_2_str(retList)))
    return retList


def load_eneity_align_dict(labelList):
    # label是给定的一个个label
    for label in labelList:
        # 得到label数据
        data = cache.get(label)

        for k in data.keys():
            name  = k
            alias = data[k]

            if common.data_is_NULL(alias):
                continue
            if isinstance(alias,list):
                for a in alias:
                    g_primary_key_align_dict.update({a:name})
            else:
                g_primary_key_align_dict.update({alias:name})

        return g_primary_key_align_dict


def word_primary_key_align(words, isExpend=True):
    global g_primary_key_align_dict

    if common.data_is_NULL(g_primary_key_align_dict):
        g_primary_key_align_dict=load_eneity_align_dict(g_align_label)

    if common.data_is_NULL(g_primary_key_align_dict):
        return words


    retWords = []
    for w in words:
        if w in g_primary_key_align_dict.keys():
            w1 = g_primary_key_align_dict[w]
            if w1 != w:
                if isExpend:
                    retWords.append(w1)
                else:
                    w = w1
        retWords.append(w)
    return retWords


# 6.一度关系扩展
# def discovery_from_property(paramList):
#     # [{'name': ['201608150434'],'describe:SDN 产品测试】【维护分支】若建立IPSEC站',details:"小需求提单",
#     # 'describekey': ['sdn', '产品', '测试', '维护', '分支', '建立', 'ipsec', '站点', '选择', 'vpn', '服务', '策略', 'ike', '崩溃'],
#     #  'detailkey': ['sdn', '管理', 'controller', '环境', '125', 'm9k', 'ipsec', '站点', '策略', 'ike', '多用户', 'open', 'stack']}
#     if common.data_is_NULL(paramList):
#         logging.info("param is null,no need kdd")
#     label=["iPDTSwitch", "iPDTSwitchSeries", "iPDTSwitchSpecs"]
#     destList=[]
#     for param in paramList:
#         describekey= param["describekey"]
#         detailkey  = param["detailkey"]
#
#         for srcList in [describekey,detailkey]:
#             for s in srcList:
#                 if isinstance(label, list):
#                     for l in label:
#                         if s in cache.g_entity_dict[l].keys():
#                             print(cache.g_entity_dict[l].keys())
                            # destList.append(s)
                # else:
                #     if s in cache.g_entity_dict[label].keys():
                #         destList.append(s)

    # return


# 7 关键字更新到原有的list里
def search_update(kdddict,list_thr):
    list_kdd = []
    for param_dict in list_thr:
        for key in kdddict.keys():
            if param_dict["name"][0] == key:
                param_dict.update({"searchkey":kdddict[key]})
            else:
                continue

            list_kdd.append(param_dict.copy())
    # print(list_kdd)
    return list_kdd


# 将将嵌套的list转换成一维的list
def flat(l):
    for k in l :
        if not isinstance(k,(list,tuple)):
            yield k
        else:
            yield from flat(k)


# 6 search关键字
def discovery_from_merge(list_thr):
    if common.data_is_NULL(list_thr):
        logging.info("数据合并失败")
    list_merge=[]
    for param in list_thr:
        kdddict = {}
        destList = []
        destname=param["name"]
        for keys in param.keys():
            if keys == "describekey" or keys == "detailkey":
                if "\\[" in param[keys] or "\\]" in param[keys]:
                    destList = destList + common.csv_list_str_2_list(param[keys])
                else:
                    destList.append(param[keys])

        # 已满足,将嵌套的list转换成一维的list
        # print(set(reduce(operator.add,destList)))

        # print(list(set(flat(destList))))

        kdddict.update({destname[0]:list(set(flat(destList)))})

        list_return = search_update(kdddict,list_thr)
        for dict_par in list_return:
            dict={
                "name":dict_par["name"][0],
                "describe":dict_par["describe"],
                "detail":dict_par["detail"],
                "describekey":dict_par["describekey"],
                "detailkey":dict_par["detailkey"],
                "searchkey":dict_par["searchkey"]
            }
        list_merge.append(dict.copy())
    return list_merge


# 5.数据合并扩展
def add_dict(line,list_sec):
    list_thr = []
    for dict_key in list_sec:
        for dict_param in line:
            if dict_param["name"]==dict_key["name"][0]:
                dict_param.update(dict_key)
            else:
                continue
            list_thr.append(dict_param.copy())
    list_merge=discovery_from_merge(list_thr)
    return list_merge


# 4.黑白名单过滤
def  white_black(list,line):
    # 黑白名单过滤
    blackListFile = issueconstruction.g_word_segment_black_file
    whiteListFile = issueconstruction.g_word_segment_white_file
    list_sec=[]

    for dict_p in list:
        name=dict_p["name"]
        for keys in dict_p.keys():
            if keys == "describekey":
                describe=words_black_list_process(blackListFile, dict_p[keys])
                dict_p.update({keys:describe})
            elif keys == "detailkey":
                detail=words_white_list_process(whiteListFile, dict_p[keys])
                dict_p.update({keys:detail})
        list_sec.append(dict_p.copy())
    list_black_white=add_dict(line,list_sec)
    return list_black_white


# 3.对简述详述进行分词并创建索引
def discovery_from_wordseg(line):
    if common.data_is_NULL(line):
        logging.info("line is null")
        return True
    # param是字典里的简述跟详述
    dict={}
    list=[]
    for param in line:
        for keys in param.keys():
            # 分词
            destList = wordSegment.run(param[keys])
            # 索引
            destList = word_primary_key_align(destList)
            if keys == "name":
                dict.update({keys: destList})
            else:
                dict.update({keys+"key":destList})
        list.append(dict.copy())

    list_detail_des=white_black(list,line)
    return list_detail_des

        # for scrString in [describe,detail]:
        #     # 数据分词
        #     destList = wordSegment.run(scrString)
        #     # 生成索引
        #     destList = word_primary_key_align(destList)
        #
        #     print(destList)
        #
        #     # 白名单
        #     # destList = words_white_list_process(whiteListFile, destList)
        #     # print(destList)
        #     # # 黑名单
        #     # destList = words_black_list_process(blackListFile, destList)
        #     # print(destList)


# 2.将清洗好的字段数据替换原有的data
def generate_output_file_overwrite(contentList, list_pm):
    line=[]
    for content_dict in contentList:
        content_dict=eval(str(content_dict))
        # 2017,2016,2015
        name=content_dict["name"]
        # pms是集合里的一个个字典
        for pms in list_pm:
            # keys是字典里的keys
            for keys in pms.keys():
                # 当2017=
                if name == pms[keys]:
                   # 更新content_dict
                   content_dict.update(eval(str(pms)))
                   line.append(content_dict)
                else:
                    continue
    # print(line)
    list_clean=discovery_from_wordseg(line)
    return list_clean


# 1.遍历需要清洗的字段并封装成list
def discovery_from_delhtmllabel(contentList):
    if common.data_is_NULL(contentList):
        logging.info("contentList is null!")
    dict_add={}
    list_pm=[]
    for param_dict in contentList:
        name = param_dict.get("name")
        dict_add["name"]=name
        for pms in ["detail"]:
            pm=param_dict.get(pms)
            destStr = ''
            if len(pm)>0:
                src_soup = BeautifulSoup(pm, 'html5lib')
                if src_soup is not None:
                    # get_text得到html内容
                    src_soup_text = src_soup.get_text()
                    if src_soup_text:
                        destStr = src_soup_text.replace('\n', '')
                        destStr = destStr.replace('\t', '')
                        destStr = re.sub('\\s+', ' ', destStr)
                        dict_add[pms]=destStr
            else:
                dict_add[pms]=' '
        list_pm.append(dict_add.copy())
    # print(list_pm)
    list_iter=generate_output_file_overwrite(contentList, list_pm)
    return list_iter
